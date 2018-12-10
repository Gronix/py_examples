# from nltk.corpus import reuters
# from nltk.tokenize import WordPunctTokenizer, sent_tokenize
# from multiprocessing.dummy import Pool as ThreadPool
# from pathos.multiprocessing import ProcessingPool as PPool
# import sys
# import importlib
from nltk.util import ngrams
from collections import Counter
from multiprocessing import Pool, Manager
import nltk.tokenize
import pickle
import gc

def KN_preproc(kgrams, kg_counts, k, n=10000000):
	kgrams_stats = []
	# count Dn's
	print("1 |", end = ' ')
	for i in range(k):
		once = 0
		twice = 0
		for _, value in kg_counts[i].most_common()[::-1]:
			if value > 2:
				break
			if value == 1:
				once += 1
			else:
				twice += 1
		kgrams_stats.append(KGrStats(i, once, twice, dict()))
		print(str(i + 1) + '...', end=' ')

	# count N's i.e (.x) and (x.)
	print("\n2 |", end = ' ')
	for i in range(0, k-1):

		for sent in kgrams[i][:n]:
			n_ = 0
			_n = 0
			j = 0
			for sent2 in kgrams[i+1]:
				if sent2[:-1] == sent:
					n_ += 1
				if sent2[1:] == sent:
					_n += 1
			params = KNConsts(sent, kg_counts[i][sent], n_, _n, i+1)
			kgrams_stats[i].upd_dct((sent, params))
		print(str(i + 1) + "...", end=' ')
	
	print("done!")
	return kgrams_stats 

class KNConsts:
	def __init__(self, word, count, n_, n_count_sum, _n, _n_, k):
		self.word = word
		self.count = count
		self.n_ = n_
		self.n_cs = n_count_sum
		self._n = _n
		self._n_ = _n_
		self.k = k


class KGrStats:
	def __init__(self, k, dictKN_consts):
		self.k = k
		self.dct = dictKN_consts

	def upd_dct(self, tup):
		self.dct.update(dict([tup]))


class KGProbs:
	def __init__(self, word, k, prob_direct, prob_continuation, y):
		self.word = word
		self.k = k
		self.prb_drct = prob_direct
		self.prb_cont = prob_continuation
		self.y = y


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


def toknz(tokz, sents_idxs):
	l = []
	for idx in sents_idxs:
		l.append(tokz(sents[idx]))
	return l


def preproc(text, n=-1):
	global sents
	if False:
		pass
	else:
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
		lwords = pool.map(nltk.tokenize.WordPunctTokenizer().tokenize, sents) #///
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


def get_kgrams_M(idxs):
	tl = []
	for idx in idxs:
		tl += list(ngrams(ll[idx], i, pad_left=True, pad_right=True, \
		 	left_pad_symbol='<s>', right_pad_symbol='</s>'))
	return tl


def single_evgr(fname, k=3):
	with open(fname, 'rb') as f:
		ll = pickle.load(f)

	kgr = []
	for i in range(1, k+1):
		_tl = []
		for sent in ll:
			_tl += list(ngrams(sent, i, pad_left=True, pad_right=True, \
				 	left_pad_symbol='<s>', right_pad_symbol='</s>'))
		kgr.append(_tl)
	print("kgrams extracted")
	with open(fname + '_res', 'wb') as f:
		pickle.dump(kgr,f)
	print("save it!")


def join_results(fnames=[], k=3):
	global kg_counts, kg_excl
	kg_counts = [Counter() for i in range(k)]
	with open('cc.bin', 'rb') as f:
		kg_counts = pickle.load(f)
	for name in fnames:
		with open('ll_r/'+ name, 'rb') as f:
			print(name +'in proc: ', end='', flush=True)
			kg = pickle.load(f)
			print('joint')
			for i in range(k):
				kg_counts[i] += kg[i] # там уже counter в файлах
			print("counters counted")
			gc.collect()
			print('kgs are deleted from memory')
	with open('cc.bin', 'wb') as f:
		pickle.dump(kg_counts, f)
	print('dumped!')
	gc.collect()
	kg_excl = [list(cl.keys()) for cl in kg_counts]
	print('exclusive kgrams are got')
	print('saving kg_excl...', end='', flush=True)
	with open('kg_ex.bin', 'wb') as f:
		pickle.dump(kg_excl, f)
	print('done.\nAll is doned! Exit')


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


def evg_4_chunk():
	for i in range(1,9+1):
		print('# work with chunk: ' + str(i) + '.bin')
		with open('ll/'+str(i)+'.bin','rb') as f:
			_l = pickle.load(f)
		print("getting counts...", end = '', flush = True)
		_kg = [Counter(gram) for gram in get_everygrams(_l, 3)]
		print('done')
		print("saving results...", end = '', flush = True)
		with open('ll_r/'+str(i)+'.bin','wb') as f:
			pickle.dump(_kg, f)
		print('done')


def prepare(ntload = False, content_fname = 'en_US.blogs.txt', p_sizes = [1000,20000], end = -1):
	global k, kg_excl, kg_counts, k_brds, need_to_load, pool

	print("start...")
	k = 3
	pool = Pool(3)
	with open(content_fname) as f:
		if ntload == True:
			need_to_load = True
			with open('lwords.bin', 'rb') as wf:
			 	lwords = pickle.load(wf)
			print("lwords loaded!")
			kgrams = get_everygrams(lwords, k)
		else:
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
k_brds = []
kg_stats = []
Ds = []
Ns = []
need_to_load = False
_c_ = 0
r = []
kg_probs = []