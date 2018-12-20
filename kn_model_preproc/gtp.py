'''
Это программа использует мультипроцессность для предобработки массива текстов.
Дальнейшая обработка и просчёт констант лингвистической модели Кнессера-Нея идёт 
в файле 'pytrie.m'.
В текстовом файле 'en_US_blogs.txt' содержатся статьи на английском друг за другом (200М)

Запуск функций подразумевается из консоли, (а не из файла), для того чтобы в реальном времени
	можно было отслеживать прогресс.

Ключевая функция prepare - выполняет все задачи уведомляя о прогрессе и сохраняя результат

Реализовано многопоточное распределение задач через Pool.map().
'''

# from nltk.util import ngrams
from collections import Counter
from multiprocessing import Pool, Manager
import nltk
import pickle
import gc


# по специфике разбиения на слова, одинарные кавычки ("'": например в "man's") являются 
# отдельным словом - нужно их удалить из каждого предложения
def join_quotes(l):
	"""Удаление двух видов кавычек из предложения - списка со словами (с объединением разделённых частей слова)"""
	n = len(l)
	# join_quotes ’ and ' 
	while "'" in l:
		c = l.index("'")
		if c == 0 or c == n - 1:
			l.pop(c)
			n -= 1
		else:
			l = l[:c-1] + [l[c-1] + l[c+1]] + l[c+2:]
			n -= 2
	while "’" in l:
		c = l.index("’")
		if c == 0 or c == n - 1:
			l.pop(c)
			n -= 1
		else:
			l = l[:c-1] + [l[c-1] + l[c+1]] + l[c+2:]
			n -= 2
	return l


# вывод после каждого логического действия нужен, чтобы отслеживать прогресс выполнения 
# (может занимать много времени)
def get_lists_of_words(text, num_of_processes):
	"""
	Получение из массива текста списка слов разбитых по предложениям и их предобработка.
	Результат сохраненяется в файл 'slw.bin' и возвращается вызыващей стороне.
	"""

	delims = []
	n = len(text)
	delims.append(-1)
	for i in range(1,3):
		delims.append(text.find('\n', int(i*n/3)))
	delims.append(n)
	print('get delimeters')

	# распараллеленное получение предложений из разбитого на куски файла
	pool = Pool(num_of_processes)
	text = [text[delims[i]+1:delims[i+1]] for i in range(3)]
	sents = pool.map(nltk.tokenize.sent_tokenize, text)
	print("sentences are received")

	sents = [x for l in sents for x in l]
	print("sentences combined")

	# super_list_of_words - это список со списками слов
	super_list_of_words = pool.map(nltk.tokenize.WordPunctTokenizer().tokenize, sents)
	print("words were derived from sentences")
	
	super_list_of_words = [join_quotes(l) for l in super_list_of_words]
	print('quotes removed (with joining of words)')

	h = re.compile('[^a-zA-Z]')
	super_list_of_words = [[h.sub('', s) for s in l] for l in super_list_of_words[:n]]
	print('excess characters removed')

	for l in super_list_of_words:
		while '' in l:
			l.remove('')
	print('cleared from zero length words')

	super_list_of_words = [list(map(lambda s: s.lower(), l)) for l in super_list_of_words]
	print('all words become lowercase')

	with open('slw.bin', 'wb') as wf:
		pickle.dump(super_list_of_words, wf)
	print("words saved")
	# sys.exit()
	return super_list_of_words


# разбивает каждое предложение (список со словами) на k-grams,
def get_everygrams(list_of_lists, n):
	"""Разбиение списка со словами на n-gram'ы с 1 до n-го порядка. Списки k-grams возвращаются в виде списка списков"""
	n_grams = []
	print("getting ngrams: ", end = '', flush = True)
	for i in range(1, n + 1):
		print(str(i) + '.. ', end = '', flush = True)
		kgrams_list = []
		for l in list_of_lists:
			kgrams_list += list(nltk.util.ngrams(l, i,
										pad_left=True,
										pad_right=True,
										left_pad_symbol='<s>',
										right_pad_symbol='</s>'))
		n_grams.append(kgrams_list)
	print('done')

	return n_grams


def main(num_of_processes = 3, n_base = 3, content_fname = 'en_US.blogs.txt'):
	"""Обработка текста, вычленение частотности каждого k-gram'а, создание списка k-grams для проверки"""
	global n, ngrams_unique, ngrams_counts

	print("start...")
	n = n_base
	try:
		with open('slw.bin', 'rb') as wf:
		 	super_list_of_words = pickle.load(wf)
		print("super_list_of_words loaded... ", end='', flush=True)
		ngrams = get_everygrams(super_list_of_words, n)
	except FileNotFoundError:
		with open(content_fname) as f:
			super_list_of_words = get_lists_of_words(f.read(), num_of_processes)
			ngrams = get_everygrams(super_list_of_words, n)
		print("super_list_of_words extracted... ", end='', flush=True)
	except:
		print('some weird error was occured! Exit.')
		return 
	print("ngrams prepared")

	ngrams_counts = [Counter(kgrams) for kgrams in ngrams]
	print("counters counted")

	ngrams_unique = []
	for k_counter in ngrams_counts:
		ngrams_unique += list(k_counter.keys())
	print("get exclusive ngrams list\nWork is done, exit.")

# т.к. в дальнейшем подразумевалось использовать эти константы в интерактивной работе то разместил
# их в глобальной области видимости (они потребовались для функций из других модулей)
n = 0
ngrams_unique = []
ngrams_counts = []

if __name__ == '__main__':
	main()