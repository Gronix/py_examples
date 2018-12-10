'''
Это программа использует мультипроцессность для предобработки массива текстов.
Дальнейшая обработка и просчёт констант лингвистической модели Кнессера-Нея идёт 
в файле 'pytrie.m'.
В текстовом файле 'en_US_blogs.txt' содержаться статьи на английском друг за другом (200М)

Запуск функций подразумевается из консоли, (а не из файла), для того чтобы в реальном времени
	можно было отслеживать прогресс.

Ключевая функция prepare - выполняет все задачи уведомляя о прогрессе и сохраняя результат

Реализовано многопоточное распределение задач через Pool.map().
'''


from nltk.util import ngrams
from collections import Counter
from multiprocessing import Pool, Manager
import nltk.tokenize
import pickle
import gc


# по специфике разбиения на слова, одинарные кавычки (man's) являются 
# отдельным словом - нужно их удалить из каждого предложения
def join_quotes(l):
	n = len(l)
	# join_quotes ’
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


def preproc(text, n=-1):
	global sents

	delims = []
	n = len(text)
	delims.append(-1)
	for i in range(1,3):
		delims.append(text.find('\n', int(i*n/3)))
	delims.append(n)
	print('get delimeters!')

	text = [text[delims[i]+1:delims[i+1]] for i in range(3)]
	sents = pool.map(nltk.tokenize.sent_tokenize, text)
	print("sentesized!")

	sents = [x for l in sents for x in l]
	print("sents joined!")

	with open('sents.bin', 'wb') as sf:
		pickle.dump(sents, sf)
	print("sentes saved!")

	print(sents[:3], flush=True)
	lwords = pool.map(nltk.tokenize.WordPunctTokenizer().tokenize, sents)
	print("wordenized!")
	
	if n == -1:
		n = len(lwords)
	lwords = [join_quotes(l) for l in lwords[:n]]
	print('quotes joined!')

	h = re.compile('[^a-zA-Z]')
	lwords = [[h.sub('', s) for s in l] for l in lwords[:n]]
	print('substended!')

	for l in lwords:
		while '' in l:
			l.remove('')
	print('cleared from zeros-lenght')

	lwords = [list(map(lambda s: s.lower(), l)) for l in lwords]
	print('lowered!')

	with open('lwords.bin', 'wb') as wf:
		pickle.dump(lwords, wf)
	print("lwords saved!")
	# sys.exit()
	return lwords


def get_everygrams(ll, k=2):
	kgrams = []
	print("getting ngrams...")
	for i in range(1, k+1):
		print(str(i) + '... ', end = '', flush = True)
		_kl = []
		for l in ll:
			_kl += list(ngrams(l, i, pad_left=True, pad_right=True, \
				left_pad_symbol='<s>', right_pad_symbol='</s>'))
		kgrams.append(_kl)
	print('done')

	return kgrams


def prepare(ntload = False, content_fname = 'en_US.blogs.txt', p_sizes = [1000,20000], end = -1):
	global k, kg_excl, kg_counts, pool

	print("start...")
	k = 3
	pool = Pool(3)
	if ntload == True:
		with open('lwords.bin', 'rb') as wf:
		 	lwords = pickle.load(wf)
		print("lwords loaded!")
		kgrams = get_everygrams(lwords, k)
	else:
		with open(content_fname) as f:
			kgrams = get_everygrams(preproc(f.read()), k)
	print("kgrams extracted")

	kg_counts = [Counter(gram) for gram in kgrams]
	print("counters counted")

	kg_excl = [list(cl) for cl in kg_counts.keys()]
	print("get exclusive kgrams list")


pool = None
k = 0
kg_excl = []
kg_counts = []
kg_stats = []

if __name__ == '__main__':
	prepare()