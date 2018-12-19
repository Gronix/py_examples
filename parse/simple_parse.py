''' 
Задача: 
	Спарсить максимально возможное количество статей с английского региона сайта news.google.com  
	по конкретному запросу 'Russia'. Затем, построить облако тегов по словам из статей.

Ограничение:
	1. Нельзя для самого парсинга использовать не стандартные библиотеки [!] (для облака тегов можно)

Примечание:
	По уму, здесь всё надо сделать по другому, использовать Requests, Scrappy, BeautifulSoup и т.п.,
	но сроки были сжаты, а задача была поставлена как есть. Парсить новости с этого сайта вообще не стоит - 
	он ограничивает выборку (max=100) и не предоставляет настроек для сортировки статей (по дате, например).

Решение:
	+ брать список новостей из news.google.com/rss вместо прямых обращений к news.google.com. 
		По этой ссылке можно передать параметр 'n' (количество статей для выдачи, но всё равно до 100), 
		запрос и регион. В ответ нам возвращает xml файл (соответственно записи упрощены и стандартизированы), 
		где уже отображены данные, которые может показать основной сайт в структурированном и удобном для 
		обработки виде.
	+ собрав все ссылки на новости - асинхронно запускаем в потоках обработчики страниц и возвращаем счётчик слов
	+ на самой странице выхватываются все предложения заключённые в <p></p> (html тег параграфа), в который 
		обычно заключается текст для отображения браузером человеку. К сожалению так в выборку могут 
		попасть слова не только из самой статьи, но и из внешнего окружения.

'''
# Python v3.6


# импорты для "бэка" задачи - только стандартные библиотеки
from urllib.request import urlopen, Request
from collections import Counter
from datetime import datetime # только для теста - вывод даты последней новости в базе
import asyncio
import re

# импорты уже для "фронта", т.е. для облака тегов
from matplotlib import pyplot as plt
from wordcloud import WordCloud as wcloud, ImageColorGenerator
from PIL import Image
import numpy as np
import io

# специальный обработчик для запуска в асинхроне блокирующей функции на множестве аргументов
def godfather_of_asyncs(func, list_of_args):
	i = 0
	loop = asyncio.get_event_loop()
	tasks = [asyncio.ensure_future(async_wrapper(func, arg)) for arg in list_of_args]
	results = loop.run_until_complete(asyncio.wait(tasks))
	results = [r.result() for r in results[0]]
	loop.close()
	return results

# запускает блокирующую функцию в отдельном потоке
async def async_wrapper(func, args):
	loop = asyncio.get_event_loop()
	_future = await loop.run_in_executor(None, func, *args)
	return _future

def get_word_counts_from_page(url, i=-1):
	# print('{}\t#{} in process...'.format(i, url))

	r = Request(url, data=None, headers={'User-Agent': cfg['User-Agent']})
	exit_loop_count = cfg['exit_loop_const']
	while(exit_loop_count):
		try:
			with urlopen(r) as p:
				page_content = p.read().decode('utf-8')
		except:
			# некоторые сайты (например 'bbc.com') могут возвращать 
			# 	абсолютно пустой ответ при слишком частых запросах => повторяем разумное кол-во раз
			#	если всё равно не получили результата - выходим с сообщением об этом
			exit_loop_count -= 1
		else:
			break

	if exit_loop_count == 0:
		print('!!! {} source not got'.format(url))
		return Counter()

	matches = re.findall(r'(?<=>)[^<]+(?=</p>)', page_content)
	c = Counter()
	for _string in matches:
		c += Counter(re.sub(r'[\W_]+', ' ', _string).lower().split())
	return c

def main(cfg):
	u = cfg['url'] + '?' + \
			'&'.join(['{}={}'.format(k,v) for k,v in cfg['params'].items()])

	# получаем список источников
	print('fetching urls from \'news.google.com\'... ', end='', flush=True)
	# client = urlopen(u)
	with urlopen(u) as client:
		content = client.read().decode('utf-8')
	# client.close()
	urls = re.findall(r'(?<=<link>)[^<]+', content)
	del urls[0] # это ссылка на адрес поиска
	print('done! {} urls was found.'.format(len(urls)))

	# выведем дату самого старого, чисто для информации
	s_form = '%a, %d %b %Y %H:%M:%S %Z'
	dates = [datetime.strptime(d, s_form) for d in re.findall(r'(?<=<pubDate>)[^<]+', content)]
	print('{} - oldest publication date'.format(sorted(dates)[0]))


	# извлекаем из них частотный словарь
	print('counting words... [please wait - about 3 minutes] ', end='', flush=True)
	words_counts = Counter()

	# параметр 'i' - для тестирования
	# urls = [(url, i) for url, i  in zip(urls, range(1, 1000000))]
	# это преобразования для того, чтобы функции-обёртки могли верно распаковать параметры для оборачиваемой функции: *par
	urls = [(url,) for url in urls]
	results = godfather_of_asyncs(get_word_counts_from_page, urls[:10])
	for res in results:
		words_counts += res
	for word in cfg['banned_words']:
		del words_counts[word]
	print('done!')

	# делаем облако тегов
	with Image.open(io.BytesIO(urlopen(cfg['image_url']).read())) as img:
		print('printing image... ', end='', flush=True)
		img_mask = np.array(img)
		wc = wcloud(width=800,
					height=165,
					min_font_size=1,
					background_color=cfg['bg_color'],
					max_words=cfg['cloud_max_wrds'], 
					mask=img_mask,
					max_font_size=50, 
					random_state=42)
		wc.generate_from_frequencies(words_counts)
		image_colors = ImageColorGenerator(img_mask)
		plt.imshow(wc.recolor(color_func=image_colors), interpolation="bilinear")
		plt.axis("off")
		plt.show()
		plt.pause(0.5)
		print('done!')

# config программы
cfg = {'params': {'q': 'Russia', 
				'as_qdr': 'm', 
				'hl': 'en-US', 
				'gl': 'US', 
				'ceid': 'US:en', 
				'num': 100}, # это максимальное количество, которым в принципе может располагать страница news.google.com по тегу
	'url': 
		'https://news.google.com/_/rss/search',
	'image_url': 
		'https://raw.githubusercontent.com/Gronix/py_examples/master/parse/Python.png',
	'bg_color': 
		'#FFFFFF',
	'cloud_max_wrds': 2000,
	'exit_loop_const': 10,
	'User-Agent': 
		'Mozilla/5.0 \
		(Macintosh; Intel Mac OS X 10_9_3) \
		AppleWebKit/537.36 (KHTML, like Gecko) \
		Chrome/35.0.1916.47 Safari/537.36',
	'banned_words': # можно было бы сделать облако более информативным подключив какой-нибудь внешний файл с, например, 1000 самых частых слов в языке
		['the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i', 'it', 
		'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this', 
		'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or', 
		'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what', 
		'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me', 
		'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know', 
		'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could', 
		'them', 'see', 'other', 'than', 'then', 'now', 'look', 'only', 
		'come', 'its', 'over', 'think', 'also', 'back', 'after', 'use', 
		'two', 'how', 'our', 'work', 'well', 'way', 'even', 'new', 'want', 
		'because', 'any', 'these', 'give', 'day', 'most', 'us', 's',
		'is', 'has', 'are', 'had', 'such', 'was', 'those', 'said', 'did', 
		'u', 't', 'were', 'more', 'been']
	}

# чтобы из интерпретатора запускать функции вручную, а из консоли работать как и запланировано.
if __name__ == "__main__":
	main(cfg)
