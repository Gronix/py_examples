'''
Программа по подсчёту констант для лингвистической модели Кнессера-Нея (3-gram)

Шаги выполнения программы:
+ Извлекается Counter для n-gram
+ Ключи если нужно модифицируются
+ Исходный массив задач разбивается на непересекающиеся отрезки (!), т.е.
    чтобы в разных корзинах не лежали n-gram'ы с общим префиксом. Это нужно
    для возможности конструирования дерева в несколько процессов (чтобы не 
    возникало ошибок синхронизации)
    И собирается в большой словарь где каждому каждому ключу соответствует 
    список пар (key-value), с единым префиксом.
+ Элементы словаря (т.е. списки) заносятся в расшаренную очередь, где её разбирают 
    worker'ы, конструируя непересекающиеся ветки этого trie
+ В конце дерево сохраняется

Обработка этих констант и просчёт уже самих вероятностей для модели дело нехитрое и вынесено 
по технической необходимости в отдельный файл, который здесь не рассматривается.

Особенность задачи в том, что т.к. данных очень много (unigram: 3 млн, bigram: 15 млн, trigram: 18 млн),
то конструирование дерева занимает тоже достаточно времени. Чтобы это ускорить - нужно задачу разделить
между процессорами. Из-за GIL в Python нельзя с помощью потоков ускорить программу. Приходится
прибегать к мультипроцессности держа в голове все издержки и особенности такого подхода.
'''


from multiprocessing import Pool, Queue, Process, Value, Lock, current_process, cpu_count
from multiprocessing.managers import BaseManager
from collections import defaultdict
import sys
import pickle

class MyManager(BaseManager): pass

def _Manager():
    m = MyManager()
    m.start()
    return m

class Trie:
    class TrieNode():
        def __init__(self, char: str):
            self.char = char # или word, без разницы. По ходу алгоритма именно слова и будут там в узлах дерева
            self.chlds = dict()
            # Is it the last character of the word.`
            self.count = 1
            self.substences_n1 = 0
            self.substences_n2 = 0
            self.substences_n3plus  = 0
            self.word_finished = False

        def create_branch(self, word: str, size_of_postfix: int, count: int):
            if size_of_postfix == 0:
                self.word_finished = True
                self.count = count
            else:
                if count == 1:
                    self.substences_n1 += 1
                elif count == 2:
                    self.substences_n2 += 1
                elif count > 2:
                    self.substences_n3plus += 1
                else:
                    print('wtf brou!? _ 2')
                ch = word[-size_of_postfix]
                self.chlds.update({ch: Trie.TrieNode(ch)})
                self.chlds[ch].create_branch(word, size_of_postfix-1, count)

        def _del(self):
            del self.chlds
            del self

    def __init__(self, root_char = '*'):
        self.root = self.TrieNode(root_char)
        self.size = 0

    def add(self, word: str, count: int = 1):
            node = self.root
            ln = len(word)
            branched = False
            for char in word:
                if char in node.chlds:
                    node = node.chlds[char]
                    if count == 1:
                        node.substences_n1 += 1
                    elif count == 2:
                        node.substences_n2 += 1
                    elif count > 2:
                        node.substences_n3plus += 1
                    else:
                        print('wtf brou!?')
                    ln -= 1
                else:
                    node.chlds.update({char : self.TrieNode(char)})
                    node.chlds[char].create_branch(word, ln-1, count)
                    branched = True
                    break
            if not branched:
                if node.word_finished:
                    # print('ERROR! word duplicate', word)
                    node.count += 1
                else:
                    node.word_finished = True
                    node.count = count
            self.size += 1

    def construct_from_counter(self, count_obj, parameter: str = 'n_'):
        if parameter == 'n_':
            for word, count in count_obj.items():
                self.add(word, count)
        elif parameter == '_n':
            for word, count in count_obj.items():
                _word = list(word)
                _word.reverse()
                self.add(_word, count)

    def construct_from_pairs(self, pairs_list):
        for word, count in pairs_list:
            self.add(word, count)

    def save(self, file_name='__trie_dump.bin'):
        with open(file_name, 'wb') as f:
            pickle.dump(self, f)

    def find_prefix(self, prefix):
        node = self.root
        if not node.chlds or prefix == '':
            return False, 0
        for char in prefix:
            char_not_found = True
            if char in node.chlds:
                node = node.chlds[char]
                char_not_found = False
            if char_not_found:
                return False, 0
        return True, node.count, node.substences_n1, node.substences_n2, node.substences_n3plus

    def get_node(self, prefix):
        node = self.root
        if not node.chlds or prefix == '':
            return False, 0
        for char in prefix:
            char_not_found = True
            if char in node.chlds:
                node = node.chlds[char]
                char_not_found = False
            if char_not_found:
                return None
        return node

    def delete(self):
        self.root._del()
        del self

MyManager.register('Trie', Trie)

def worker(shared_trie, count_of_jobs, jobs_done_at_this_time, prev_done_percent, jobs_queue, lock_obj):
    while True:
        job = jobs_queue.get()
        if job == None:
            break
        shared_trie.construct_from_pairs(job)

        lock_obj.acquire()
        try:
            jobs_done_at_this_time.value += 1
            cur_percent = int(jobs_done_at_this_time.value/count_of_jobs.value * 100)
            if cur_percent > prev_done_percent.value + 4:
                print(str(current_process().name) + ' proc done with', jobs_done_at_this_time.value, 'from', count_of_jobs.value, \
                    'at all (' + str(cur_percent) + '%)')
                sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
                prev_done_percent.value = cur_percent
        finally:
            lock_obj.release()

def parallel_gets_jobs(pairs_list):
    jobs_dict = defaultdict(list)
    for k,v in pairs_list:
        key = k[0]
        jobs_dict[key] += [(k,v)]
    print('| ', end = '', flush = True)
    sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
    return jobs_dict

def construct_dict_with_modified_keys(dct, mode='rev',k=3):
    def _rev3(tup_key):
        return tup_key[2], tup_key[1], tup_key[0]
    def _mid3(tup_key): # ONLY FOR tup_key LENGHT = 3
        return tup_key[1], tup_key[0], tup_key[2]
    def _rev2(tup_key):
        return tup_key[1], tup_key[0]

    dct_new = {}
    if k == 3:
        if mode == 'rev':
            key_modif = _rev3
            print('rev 3 mode set')
        else:
            key_modif = _mid3
            print('mid 3 mode set')
    else:
        key_modif = _rev2
        print('rev 2 mode set')
        
    sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
    for k,v in dct.items():
        k_new = key_modif(k)
        dct_new.update({k_new : v})

    return dct_new

# FILE_NAME    CPUs    PARTS_AT_CPU    MODIFY_KEYS?    LOAD_counter_list?
#     1         2           3               4                   5 (просто _)
if __name__ == "__main__":
    with open(sys.argv[1], 'rb') as f:
        q = Queue()
        if len(sys.argv) < 3:
            num_of_working_cpus = cpu_count()
            num_of_tasks = 3
        else:
            num_of_working_cpus = int(sys.argv[2])
            num_of_tasks = int(sys.argv[3])
        pool = Pool(num_of_working_cpus)

        if len(sys.argv) != 6:
            counter_obj = pickle.load(f)            
            print('file loaded')
            sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

            k = len(list(counter_obj.keys())[0])
            print('k=' + str(k))
            sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

            brd_count = num_of_working_cpus * num_of_tasks
            n = len(counter_obj)
            boards = [int(i*n/brd_count) for i in range(brd_count)]
            boards.append(n)

            # Если нужно, тогда модифицируем ключи (например, для подсчёта некоторых констант 
            # trie дерево должно быть построено на обратных 2-3-грамах)
            if len(sys.argv) >= 4 and sys.argv[4] != 'no':
                _counter_obj = construct_dict_with_modified_keys(counter_obj, sys.argv[4], k)
                del counter_obj
                counter_obj = _counter_obj
                del _counter_obj

            _counts = list(counter_obj.items())
            del counter_obj
            counters_list = [[_counts[boards[i]:boards[i+1]]] for i in range(brd_count)]
            with open('counter_list.bin','wb') as f:
                pickle.dump(counters_list, f)
            del _counts
        else:   
            counters_list = pickle.load(f)
            brd_count = len(counters_list)
            print('file loaded')

        print('constructed counter_list for parallel calculationg jobs!')
        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        tl = [pool.apply_async(parallel_gets_jobs, counters_part) for counters_part in counters_list]

        print('jobs construction (' + str(brd_count) + ' separated tasks): ', end = '', flush=True)
        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
        for task in tl:
            task.wait()
        print(' calculated')

        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
        i = 0
        jobs_dict = {}

        print('work with...')
        for dct in [res.get() for res in tl]:
            print(' ' + str(i+1) + ' part: ', end='', flush=True)
            sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
            if i == 0:
                jobs_dict = dct
            else:
                j = 0
                next_done_percent_border = 10
                n = len(dct)
                for k, list_of_values in dct.items():
                    jobs_dict[k] += list_of_values
                    j += 1
                    if int(100*j/n) == next_done_percent_border:
                        print(str(int(100*j/n)) + '% ', end='', flush=True)
                        next_done_percent_border += 10
            i += 1
            print('inserted')
            sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        pool.close()
        pool.join()
        print('jobs dict constructed')
        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        for v in jobs_dict.values():
            q.put(v)
        print(str(len(jobs_dict)) + ' jobs putted into queue')
        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        v_jobs_count = Value('i', len(jobs_dict), lock=False)
        v_jobs_done_incr = Value('i', 0, lock=False)
        v_jobs_done_pref_procent = Value('i', 0, lock=False)

        lock = Lock()
        manager = _Manager()
        shared_trie_instance = manager.Trie()
        # task_list = [pool.apply_async(trie.construct_from_pairs, (shared, v_jobs_count, v_jobs_done_incr, q, lock)) for tup in k_brds[0]]
        for cpu in range(num_of_working_cpus):
            q.put(None)

        arguments = (shared_trie_instance, v_jobs_count, v_jobs_done_incr, v_jobs_done_pref_procent, q, lock)
        procs = []
        print('all arguments and variables prepared')
        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        for cpu in range(num_of_working_cpus):
            proc = Process(target=worker, args=arguments)
            procs.append(proc)
            proc.start()
            print(str(cpu + 1) + ' process from ' + str(num_of_working_cpus) + ' are started')
            sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD

        for p in procs:
            p.join()
        
        print('saving results... ', end='', flush=True)

        sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
        shared_trie_instance.save()
        print('done!')
        print('EXIT')
