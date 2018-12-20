'''
Программа по подсчёту констант для лингвистической модели Кнесера-Нея (3-gram)

Шаги выполнения программы:
+ Извлекается Counter для n-gram
+ Ключи если нужно модифицируются (для извлечения в дальнейшем разных коэффициентов)
+ Исходный массив задач разбивается на непересекающиеся отрезки (!), 
    [т.е. чтобы в разных корзинах не лежали n-gram'ы с общим префиксом. Это 
    нужно для возможности конструирования дерева в несколько процессов, так 
    не возникает ошибок синхронизации]
    И собирается в большой словарь где каждому каждому ключу соответствует 
    список пар (k-gram(=префикс) : [список полдолжений]).
+ Элементы словаря (т.е. списки пар) заносятся в расшаренную очередь, где её разбирают 
    worker'ы, конструируя непересекающиеся ветки этого trie.
+ В конце дерево сохраняется.

Обработка этих констант и просчёт уже самих вероятностей для модели дело нехитрое и вынесено 
по технической необходимости в отдельный файл, который здесь не рассматривается.

Особенность задачи в большом количестве данных (unigram: 3 млн, bigram: 15 млн, trigram: 18 млн),
из-за него конструирование дерева занимает достаточно много времени. 
Чтобы это исправить программа была распараллелена.
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

def simple_job_creator(pairs_list):
    jobs_dict = defaultdict(list)
    for k,v in pairs_list:
        key = k[0]
        jobs_dict[key] += [(k,v)]
    print('| ', end = '', flush = True)
    sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
    return jobs_dict


# для ключей словаря, обрабатывать которые не нужно - функция вызываться не должна
def modify_dict_keys(dct, mode = 'rev', k = 3):
    """Создание нового словаря с модифицированными ключами: 
        'rev' - перевернётые, [(3,2,1) и (2,1)]
        'mid' - "подвешенные" за середину (только при k == 3 (!)) [(2,1,3)]
    """
    def _rev(tup_key):
        return tuple(reversed(tup_key))
    def _mid3(tup_key):
        return tup_key[1], tup_key[0], tup_key[2]

    dct_new = {}
    if k == 3:
        if mode == 'rev':
            key_modif = _rev
            print('rev 3 mode set')
        else:
            key_modif = _mid3
            print('mid 3 mode set')
    else:
        key_modif = _rev
        print('rev 2 mode set')
        
    sys.stdout.flush() # FOR NOHUP IN GOOGLE CLOUD
    for k,v in dct.items():
        k_new = key_modif(k)
        dct_new.update({k_new : v})

    return dct_new

# регистрация класса, к объекту которого будет предоставляться доступ в общей для процессов памяти
MyManager.register('Trie', Trie)

def split_counter_to_buckets(counter_obj, args, n):
    '''Разбиение общего частотного словаря по корзинам с обработкой ключей, для дальнейших паралелльных вычислений'''

    print('creating jobs...', end='', flush=True)
    boards_num = args.cpusn * args.partsn
    size = len(counter_obj)
    boards = [int(i * size / boards_num) for i in range(boards_num)]
    boards.append(size)

    # Если нужно, тогда модифицируем ключи (например, для подсчёта некоторых констант 
    # trie дерево должно быть построено на обратных 2-3-грамах)
    if args.key_mode != 'no':
        counter_obj = modify_dict_keys(counter_obj, args.key_mode, args.k)

    # разбиваем получившийся частотный словарь на списки для pool.map (конструировать)
    items_in_buckets = list(counter_obj)
    items_in_buckets = [ [ items_in_buckets[boards[i] : boards[i+1]] ] for i in range(boards_num)]

    return items_in_buckets


def jobs_construct(items_in_buckets, num_of_working_cpus):
    '''Конструирование словаря n-gram у которых общий единичный префикс (первый токен)'''

    pool = Pool(num_of_working_cpus)
    task_list = [pool.apply_async(simple_job_creator, bucket) for bucket in items_in_buckets]
    for task in task_list:
        task.wait()
    print('dict with common prefixes done. ', end='', flush=True)

    first_phase = True
    jobs_dict = {}
    for dct in [task.get() for task in task_list]:
        if first_phase:
            jobs_dict = dct
            first_phase = false
        else:
            n = len(dct)
            for prefix, continuation_list in dct.items():
                jobs_dict[prefix] += continuation_list
    print('parts joined. ', end='', flush=True)
    pool.close()
    pool.join()
    return jobs_dict


def overseer_on_workers(num_of_cpus, workers_args):
    '''Запуск "рабочих" по числу процессоров из "CPUs num", ожидание завершения их работы'''
    
    # чтобы каждый процесс получил своё None-значение сигнализирующее о завершении работы
    for i in range(num_of_cpus):
        q.put(None)

    processes = []
    for cpu in range(num_of_cpus):
        proc = Process(target=worker, args=workers_args)
        processes.append(proc)
        proc.start()

    # ждём пока работа выполнится
    for p in processes:
        p.join()


def arg_parser_configurator():
    '''Настройка 'argparse' - стандартного парсера коммандной строки python.'''

    arg_parser = argparse.ArgumentParser(description='Main goal of this program is \
                            preparing to calculate Kneser-Ney language model constants from Trie.\
                            Execute only from terminal.')
    arg_parser.add_argument('file_name', 
                        type=str,
                        help='Name of file contains counts for n-grams, processed \
                        in previous stage of algo.')
    arg_parser.add_argument('-k', '--k_gram', 
                        dest='k', action='store', type=int, default=1,
                        help='Current k-gram part of n-grams to build Trie.')
    arg_parser.add_argument('-c', '--cpus', 
                        dest='cpusn', action='store', type=int, default=3,
                        help='Number of CPUs to use.')  
    arg_parser.add_argument('-p', '--parts_num', 
                        dest='partsn', action='store', type=int, default=3,
                        help='Number of data chunks processed per one CPU.')
    arg_parser.add_argument('-m', '--keys_modify', 
                        dest='key_mode', action='store', type=str, 
                        choices=['rev', 'mid', 'no'], default='no',
                        help='Modify dictionary keys mode.\
                        rev = inverted,\
                        mid = middle part at first [2,1,3],\
                        no = stay as is (default))')
    return arg_parser


def main(arg_parser):
    '''Распараллеленное конструирование Trie для дальнейшего подсчёта коэффициентов \
    языковой модели из частотного словаря k-gram'''

    # arg_parser = arg_parser_configurator()
    args = arg_parser.parse_args()
    __doc__ = arg_parser.description

    try:
        with open(arg_parser.fname) as file:
            counters_list = pickle.load(file)
            print('split counter to buckets...', end='', flush=True)
            buckets_of_counter_items = split_counter_to_buckets(counters_list[args.k], args)
            del counters_list
            print('done')

    except FileNotFoundError:
        print('File with name "{}" was not found! Exit.'.format(arg))
        sys.exit()
    except:
        print('Some strange thing was occured... Closing.')
        sys.exit()

    print('jobs construction... ', end='', flush=True)
    jobs_construct(buckets_of_counter_items, args.cpusn)
    print('done.')

    print('preparing shared variables... ', end='', flush=True)
    q = Queue()
    for v in jobs_dict.values():
        q.put(v)
    print('[{} jobs in queue] '.format(len(jobs_dict)), end='', flush=True)

    # общие переменные только для интерактивного отслеживания прогресса
    v_jobs_count = Value('i', len(jobs_dict), lock=False)
    v_jobs_done_increment = Value('i', 0, lock=False)
    v_jobs_done_previous_percent = Value('i', 0, lock=False)

    lock = Lock() # будет блокировать только изменение v_jobs*'ов
    manager = _Manager() # экземпляр менеджера класса
    shared_trie_instance = manager.Trie() # экземпляр Trie в общей памяти под управлением MyManager
    worker_args = (shared_trie_instance, v_jobs_count, v_jobs_done_incr, v_jobs_done_pref_procent, q, lock)
    print('done')

    print('begining of the work "day"...', end=False, flush=True)
    overseer_on_workers(args.cpusn, worker_args)
    print('done')

    print('saving results... ', end='', flush=True)
    shared_trie_instance.save()
    print('done\nExit')

arg_parser = arg_parser_configurator()
if __name__ == "main":
    main(arg_parser)
else:
    arg_parser.print_help()