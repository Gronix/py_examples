import pickle
import sys

class Trie:
	def _none(self, trie_base_element):
		return trie_base_element

	def _rev(self, trie_base_element):
		return trie_base_element[::-1]


	class TrieNode():
		_delim = ''

		def __init__(self, char, parent = None):
			self.char = char
			self.chlds = dict()
			self.count = 1
			self.parent = parent
			self.word_finished = False


		def __len__(self):
			return len(self.chlds)


		def create_branch(self, word: str, size_of_postfix: int):
			if size_of_postfix == 0:
				self.word_finished = True
			else:
				ch = word[-size_of_postfix]
				self.chlds.update({ch: Trie.TrieNode(ch, self)})
				self.chlds[ch].create_branch(word, size_of_postfix-1)

		# BAAAAAAAAAAAAAAADDDDDDDDDDDDDDDD
		def get_childs_map(self, depth=1000000):
			if depth == 0 or len(self.chlds) == 0:
				s = self.char
				if self.count > 1:
					s = '[{}]'.format(s)
				return [s]
			nodes_map = []
			for child_node in self.chlds.values():
				sons = child_node.get_childs_map(depth-1)
				node_char = self.char
				if self.word_finished:
					node_char  = '({})'.format(node_char)
				for son in sons:
					nodes_map.append(node_char + self._delim + son)
			nodes_map = sorted(nodes_map)
			return nodes_map


		def _del(self):
			del self.chlds
			del self


	def __init__(self, root_char = '*', delim = ''):
		self.root = self.TrieNode(root_char, None)
		self.root.count = 0
		self._key_modifier = self._none
		self._default_filename = '__trie_dump.bin'
		# self._delim = delim


	def __contains__(self, word):
		node = self.root
		if not node.chlds or word == '':
			return False
		for char in word:
			if char in node.chlds:
				node = node.chlds[char]
			else:
				return False
		if node.word_finished:
			return True
		return False


	def __len__(self):
		return self.root.count
			

	def add(self, word):
		if word == '':
			return
		node = self.root
		ln = len(word)
		word = self._key_modifier(word)
		branched = False
		for char in word:
			node.count += 1
			if char in node.chlds:
				node = node.chlds[char]
				ln -= 1
			else:
				node.chlds.update({char : self.TrieNode(char, node)})
				node.chlds[char].create_branch(word, ln-1)
				branched = True
				break
		if not branched and not node.word_finished:
			node.word_finished = True


	def add_more(self, iterable):
		for item in iterable:
			self.add(item)


	def reverse_keys_mode_switcher():
		self._key_modifier = self._rev

	def change_delimeter(self, new_delim = ''):
		self.TrieNode._delim = new_delim


	def save(self, file_name=None):
		if not file_name:
			file_name = self._default_filename
		with open(file_name, 'wb') as f:
			pickle.dump(self, f)


	def load(self, file_name=None):
		if not file_name:
			file_name = self._default_filename
		with open(file_name, 'rb') as f:
			self = pickle.load(f)


	def get_prefix(self, prefix):
		node = self.root
		if not node.chlds:
			return 0
		if prefix != '':
			for char in prefix:
				if char in node.chlds:
					node = node.chlds[char]
				else:
					return 0, []

		child_nodes_map = []
		if len(node.chlds):
			for child_node in node.chlds.values():
				child_nodes_map += child_node.get_childs_map()

		return node.count, child_nodes_map


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


def main():
	pass

if __name__ == "main":
	main()