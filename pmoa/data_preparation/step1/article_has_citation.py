import logging

from nltk.tokenize import sent_tokenize
import re

from lxml import etree
import json

import nltk

nltk.data.path.append('/home/ubuntu/nltk_data')
logging.basicConfig(filename='logs/mpoa_xml_extract.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')


def read_file(path):
    """read file from specified path

    Args:
        path (str): path

    Returns:
        str: the contents

    """
    with open(path, mode='r') as file:
        text = file.read()
        return text


def get_json_from_file(path):
    with open(path, mode='r') as json_file:
        obj = json.load(json_file)
        return obj


def gen_etree(text):
    if not (text and isinstance(text, str)):
        raise Exception('input require to be a str')
    try:
        text = strip_xref_quote(text)
        tree = etree.fromstring(text, etree.XMLParser(recover=True, remove_blank_text=True))
    except Exception as e:
        print("Error: fail to generate an element tree from the input str")
        raise
    return tree


def strip_xref_quote(text):
    text = text.replace('[<xref', '<xref')
    text = text.replace('xref>]', 'xref>')
    text = text.replace('(<xref', '<xref')
    text = text.replace('xref>)', 'xref>')
    return text


def get_sample_articles_as_etree(start, end):
    article_list = get_json_from_file('../data/articles.json')
    etree_list = [gen_etree(ele_tree) for ele_tree in article_list[start:end]]
    return etree_list


def get_etree_with_path(path):
    text = read_file(path)
    etree = gen_etree(text)
    return etree


X_REF_MARK_REGEX = re.compile(r"\{\[\<\w+\>\]\}")
# X_REF_GROUP_MARK_REGEX = re.compile(r"(\s*\{\[\<\w+\>\]\}\s*[,-]*)+")
X_REF_GROUP_MARK_REGEX = re.compile(r"(\s*\{\[\<[\w,\.\-\–]+\>\]\}\s*[,\-\–]*)+")


def split_str_num(str_num):
    str_char = ''
    num_char = ''
    for s in str_num:
        if s.isdigit():
            num_char += s
        else:
            str_char += s
    try:
        num = int(num_char)
    except Exception as e:
        print('str_num=' + str_num)
        num = None
    return (str_char, num)


def getRefListWithPmid(treeRoot):
    # expr='//back/ref-list/ref/element-citation/pub-id[@pub-id-type="pmid"]/../..'
    expr = '//back/ref-list/ref[.//pub-id[@pub-id-type="pmid"]]'
    refList = treeRoot.xpath(expr)
    return refList


def getRefidPmidMap(root):
    refidPmidMap = {}
    pubIdExpr = './/pub-id[@pub-id-type="pmid"]'
    refList = getRefListWithPmid(root)
    for ref in refList:
        refId = ref.get('id')
        pubIdList = ref.xpath(pubIdExpr)
        if (len(pubIdList) > 1):
            print('exception: node ref expected to have only one pub-id')
        else:
            pmid = pubIdList[0].text
            refidPmidMap[refId] = pmid
    return refidPmidMap


class Journal:
    pass


class Person:
    pass


class Reference:
    pass


class XRef:
    def __init__(self, rid):
        self.rid = rid
        self.text = rid

    def __str__(self):
        return self.rid

    def get_rid(self):
        return self.rid


class Operator:
    def __init__(self, text):
        self.text = text


class XRefGroup:
    def __init__(self, origin_str):
        self.origin_str = origin_str.strip() if origin_str else ''
        self.xref_list = self.get_x_ref_obj_list(self.origin_str)
        self.xref_pmid_set = None

    def __str__(self):
        ref_text = ','.join(str(xref) for xref in self.xref_list)
        return '[' + ref_text + ']'

    def get_xref_pmid_set(self, rid_pmid_map, allow_cache):
        if allow_cache:
            if self.xref_pmid_set:
                return self.xref_pmid_set
        ref_set = set()
        for xref in self.xref_list:
            rid = xref.get_rid()
            pmid = rid_pmid_map.get(rid)
            if pmid:
                ref_set.add(pmid)
        self.xref_pmid_set = ref_set
        return ref_set

    def get_xref_list(self):
        return self.xref_list

    def get_x_ref_obj_list(self, origin_str):
        obj_list = self.split_x_ref(origin_str)
        x_ref_obj_list = self.parse_x_ref(obj_list)
        return x_ref_obj_list

    def split_x_ref(self, xref_str):
        # xref_str = self.text
        if not xref_str:
            return []
        matches = X_REF_MARK_REGEX.finditer(self.origin_str)
        obj_list = []
        ref_start = 0
        ref_end = 0
        for match in matches:
            match_pos = match.span(0)
            ref_start = ref_end
            ref_end = match_pos[0]
            str_before_ref = xref_str[ref_start:ref_end].strip()
            if str_before_ref:
                if str_before_ref == ',' or str_before_ref == '-' or str_before_ref == '–':
                    operator = Operator(str_before_ref)
                    obj_list.append(operator)
                else:
                    print('Exception: unexpected operator! The operator is:' + str_before_ref)
            ref_start = ref_end
            ref_end = match_pos[1]
            match_text = xref_str[ref_start:ref_end].strip()
            # 获取rid
            rid = match_text[3:-3]
            operand = XRef(rid)
            obj_list.append(operand)
        return obj_list

    def parse_x_ref(self, obj_list):
        if not (obj_list and isinstance(obj_list, list)):
            return []
        operand_stack = []
        operator_stack = []
        xref_obj_list = []
        for i in range(len(obj_list)):
            curr = obj_list[i]
            if isinstance(curr, Operator):
                if i == 0:
                    print('The first item should not be Operator')
                if len(operator_stack) > 0:
                    raise Exception('Error: there should be only one operator in the operator_stack')
                if (curr.text == ',' or curr.text == '-' or curr.text == '–'):
                    operator_stack.append(curr)
                else:
                    print('Error: un-know operator, it is:' + curr.text)
            elif (isinstance(curr, XRef)):
                if len(operand_stack) == 0:
                    if len(operator_stack) > 0:
                        operator_stack.pop(0)
                        # print('Error: lack of first operand')
                    operand_stack.append(curr)
                elif len(operand_stack) == 1:
                    if len(operator_stack) == 0:
                        # add previos operand
                        pop_xref = operand_stack.pop()
                        xref_obj_list.append(pop_xref)
                        # add curr to operand_stack
                        operand_stack.append(curr)
                    elif len(operator_stack) == 1:
                        operand = operand_stack.pop()
                        operator = operator_stack.pop()
                        if (operator.text == ','):
                            xref_obj_list.append(operand)
                            operand_stack.append(curr)
                        elif (operator.text == '-'):
                            start_rid = operand.get_rid()
                            end_rid = curr.get_rid()
                            if not (start_rid and end_rid):
                                print('start_rid=' + start_rid + ', end_rid=' + end_rid)
                                continue
                            (start_prefix, start_num) = split_str_num(start_rid)
                            (end_prefix, end_num) = split_str_num(end_rid)
                            if (start_prefix == end_prefix):
                                if start_num and end_num:
                                    for j in range(start_num, end_num + 1):
                                        new_rid = start_prefix + str(j)
                                        xref_obj_list.append(XRef(new_rid))
                                else:
                                    print('Error: (start_num and end_num) not True')
                            else:
                                print('Error: start_prefix and end_prefix not equal')
                    else:
                        raise Exception('Error: there should be one operator in the operator_stack')
                else:
                    raise Exception('Error: there should be one operand in the operand_stack')
        if len(operand_stack) > 0:
            last_one = operand_stack.pop()
            xref_obj_list.append(last_one)

        return xref_obj_list


class Span:
    def __init__(self, text):
        self.text = text

    def __str__(self):
        return self.text

    def get_text(self):
        return self.text

    @property
    def isEmpty(self):
        if self.text:
            return False
        else:
            return True


class SpanXRef:
    def __init__(self, span, xref_group):
        if not (isinstance(span, Span) and isinstance(xref_group, XRefGroup)):
            print('Error: span should be a Span instance, xref_group should be a XRefGroup instance')
        self.span = span
        self.xref_group = xref_group

    def get_xref_pmid_set(self, rid_pmid_map, allow_cache):
        return self.xref_group.get_xref_pmid_set(rid_pmid_map, allow_cache)

    def __str__(self):
        return str(self.span)

    def get_text(self):
        return self.span.text

    def get_xref_list(self):
        return self.xref_group.get_xref_list()


class Sentence:
    def __init__(self, origin_str):
        self.origin_str = origin_str or ''
        self.segment_list = []
        self.xref_pmid_set = None
        self.children = None
        self.has_citations = 0

    def __str__(self):
        str_rep = ''
        for seg in self.segment_list:
            if isinstance(seg, XRefGroup):
                str_rep += str(seg) + ' '
            else:
                str_rep += str(seg)
        return str_rep

    def to_string(self, with_xref_group=True):
        str_rep = ''
        if not with_xref_group:
            for seg in self.segment_list:
                if isinstance(seg, Span):
                    str_rep += str(seg)
                elif isinstance(seg, XRefGroup):
                    str_rep += ' '
            return str_rep
        else:
            return self.__str__()

    def get_xref_pmid_set(self, rid_pmid_map, allow_cache):
        if allow_cache and self.xref_pmid_set:
            return self.xref_pmid_set
        sentence_xref_pmid_set = set()
        for seg in self.segment_list:
            if (isinstance(seg, XRefGroup)):
                pmid_set = seg.get_xref_pmid_set(rid_pmid_map, allow_cache)
                sentence_xref_pmid_set = sentence_xref_pmid_set | pmid_set
        self.xref_pmid_set = sentence_xref_pmid_set
        return sentence_xref_pmid_set

    def get_segment_list(self):
        return self.segment_list

    def get_children(self):
        if not self.children:
            self.children = self.gen_spanxref_list()
        return self.children

    def gen_spanxref_list(self):
        segment_list = self.segment_list
        prev = None;
        curr = None;
        spanxref_list = []
        for i in len(segment_list)[1:]:
            curr = segment_list[i]
            if (isinstance(curr, XRefGroup)):
                prev = segment_list[i - 1]
                if (isinstance(prev, Span)):
                    spanxref = SpanXRef(prev, curr)
                    spanxref_list.append(spanxref)
                else:
                    print('the previous segment expected to be Span')
        return spanxref_list

    def get_first_segment(self):
        return self.segment_list[0]

    def del_first_segment(self):
        return self.segment_list.pop(0)

    def concat_origin_str(self):
        full_str = ''
        for segment in self.segment_list:
            if (isinstance(segment, Span)):
                full_str = full_str + str(segment)
            else:
                full_str = full_str + segment.origin_str
        return full_str

    def add(self, segment):
        if (isinstance(segment, Span)):
            if (not segment.isEmpty):
                self.segment_list.append(segment)
        elif (isinstance(segment, (XRefGroup))):
            self.segment_list.append(segment)
            self.has_citations = 1
        else:
            # throw exception
            print('exception: only Span and XRefGroup could be added')
            pass

    def isEmpty(self):
        if (len(self.segment_list) > 0):
            return False
        else:
            return True


class Paragraph:
    def __init__(self, p_node, section):
        self.section = section
        self.xref_pmid_set = None
        self.sentence_list = self.generate_sentence_list(p_node)

    def generate_sentence_list(self, p_node):
        inner_text = self.get_inner_text(p_node)
        sentence_list_str = self.gen_sentence_list_str(inner_text)
        sentence_list_obj = self.gen_sentence_list_obj(sentence_list_str)
        sentence_list = self.adjust_ref_mark(sentence_list_obj)
        return sentence_list

    def gen_sentence_list_str(self, full_text):
        sentence_list = sent_tokenize(full_text)
        return sentence_list

    def gen_sentence_list_obj(self, sentence_list):
        if not (sentence_list and isinstance(sentence_list, list)):
            return []
        sentence_obj_list = []
        for sentence in sentence_list:
            matches = X_REF_GROUP_MARK_REGEX.finditer(sentence)
            span_start = 0
            span_end = 0
            sentence_obj = Sentence(sentence)
            for match in matches:
                match_pos = match.span(0)
                span_start = span_end
                span_end = match_pos[0]
                str_before_ref = sentence[span_start:span_end]
                span_obj = Span(str_before_ref)
                sentence_obj.add(span_obj)

                span_start = span_end
                span_end = match_pos[1]
                ref_mark_str = sentence[span_start:span_end]
                x_ref_group_obj = XRefGroup(ref_mark_str)
                sentence_obj.add(x_ref_group_obj)
            span_start = span_end
            last_str = sentence[span_start:]
            if (last_str):
                span_obj_last = Span(last_str)
                sentence_obj.add(span_obj_last)
            sentence_obj_list.append(sentence_obj)
        return sentence_obj_list

    def adjust_ref_mark(self, sentence_obj_list):
        if not (sentence_obj_list and isinstance(sentence_obj_list, list)):
            return []
        for i in range(len(sentence_obj_list)):
            if i == 0:
                continue
            curr = sentence_obj_list[i]
            prev = sentence_obj_list[i - 1]
            curr_first_segment = curr.get_first_segment()
            if (isinstance(curr_first_segment, XRefGroup)):
                curr.del_first_segment()
                prev.add(curr_first_segment)
        return sentence_obj_list

    def getInnerXml(self, pNode):
        return etree.tostring(pNode)

    def get_inner_text(self, pNode):
        if (pNode is None):
            return ''
        text = pNode.text or ''
        childrenList = list(pNode)
        if (len(childrenList) == 0):
            if (pNode.tag == 'xref' and (pNode.get('ref-type') == 'bibr')):
                rid = pNode.get('rid') or ''
                xrefText = '{[<' + rid + '>]}' if rid else ''
                text = xrefText
            else:
                pass
        else:
            for child in childrenList:
                ret = self.get_inner_text(child)
                text = text + ret + (child.tail or '')
        return text

    def getFullText(self, pNode):
        text = pNode.text or ''
        # p may contain other complex node, such as table-wrap, another sec, we're not going recursively to make things simple
        for p in list(pNode):
            if (p is not None):
                if (p.tag == 'xref' and pNode.get('ref-type') == 'bibr'):
                    rid = p.get('rid')
                    xrefText = '{' + rid + '}' if rid else ''
                    text = text + xrefText + (p.tail or '')
                else:
                    text = text + (p.text or '') + (p.tail or '')
        return text

    def __str__(self):
        new_str = ''
        for sentence in self.sentence_list:
            new_str += str(sentence)
        return new_str

    def to_string(self, with_xref_group=True):
        str_rep = ''
        if not with_xref_group:
            for sentence in self.sentence_list:
                str_rep += sentence.to_string(with_xref_group)
            return str_rep
        else:
            return self.__str__()

    def get_xref_pmid_set(self, rid_pmid_map, allow_cache):
        if allow_cache and self.xref_pmid_set:
            return self.xref_pmid_set
        p_xref_pmid_set = set()
        for sentence in self.sentence_list:
            p_xref_pmid_set = p_xref_pmid_set | sentence.get_xref_pmid_set(rid_pmid_map, allow_cache)
        self.xref_pmid_set = p_xref_pmid_set
        return p_xref_pmid_set


class Section:
    def __init__(self, sec_node, article):
        self.article = article
        self.id = ''
        self.title = self.get_section_title(sec_node)
        self.paragraph_list = self.gen_paragraph_list(sec_node)
        self.xref_pmid_set = None

    def get_section_title(self, sec_node):
        title_xpath = './title'
        title_node = sec_node.xpath(title_xpath)
        if len(title_node) > 1:
            print('exception: more than two titles in a section')
        try:
            first_title_node = title_node[0]
            title = first_title_node.text
        except:
            title = ''
        return title

    def get_xref_pmid_set(self, rid_pmid_map, allow_cache):
        if allow_cache and self.xref_pmid_set:
            return self.xref_pmid_set
        sec_pmid_set = set()
        for p in self.paragraph_list:
            sec_pmid_set = sec_pmid_set | p.get_xref_pmid_set(rid_pmid_map, allow_cache)
        self.xref_pmid_set = sec_pmid_set
        return sec_pmid_set

    def __str__(self):
        new_str = self.title + '\n'
        for p in self.paragraph_list:
            new_str += str(p)
        return new_str

    def to_string(self, with_xref_group=True):
        str_rep = ''
        if not with_xref_group:
            for p in self.paragraph_list:
                str_rep += p.to_string(with_xref_group)
            return str_rep
        else:
            return self.__str__()

    def gen_paragraph_list(self, secNode):
        p_arr = []
        p_xpath = './p'
        p_list = secNode.xpath(p_xpath)
        i = 0
        for p in p_list:
            i = i + 1
            # print(secNode.get('id')+', p:'+str(i))

            paragraph = Paragraph(p, self)
            # print(str(paragraph))
            p_arr.append(paragraph)
        return p_arr


class Article:
    def __init__(self, root):
        self.pmid = self._get_pmid(root)
        self.journal = ''.join(root.xpath('//front/journal-meta//journal-title[1]//text()'))
        self.title = ''.join(root.xpath('//front/article-meta//article-title[1]//text()'))
        self.author_list = []
        self.abstract = '\n'.join(root.xpath('//front//abstract[1]//text()'))
        self.section_list = self.gen_sec_list(root)
        self.reference_list = []
        self.rid_pmid_map = self._get_rid_pmid_map(root)
        self.xref_pmid_set = None
        # (self.article_groups, self.section_groups, self.paragraph_groups,
        #  self.sentence_groups) = self.build_rid_pmid_map_all_level()
        (self.article_text_ref_list, self.section_text_ref_list,
         self.sentence_text_ref_list) = self.build_meta_list_all_level()

    def build_meta_list_all_level(self):
        article_meta_list = []
        section_meta_list = []
        sentence_meta_list = []
        article_id = self.pmid
        section_num = -1

        article_meta_item = {'pmid': article_id,
                             'citations': self.get_xref_pmid_set(True)
                             }
        article_meta_list.append(article_meta_item)
        for section in self.section_list:
            section_num += 1
            paragraph_num = -1
            sec_meta_item = {'pmid': article_id,
                             'secid': section_num,
                             'section_name': section.title,
                             # section.to_string(with_xref_group=False),
                             'citations': section.get_xref_pmid_set(self.rid_pmid_map, True)
                             }
            section_meta_list.append(sec_meta_item)
            for plist in section.paragraph_list:
                paragraph_num += 1
                sentence_num = -1
                for sentence in plist.sentence_list:
                    sentence_num += 1
                    sentence_meta_item = {'pmid': article_id,
                                          'secid': section_num,
                                          'paraid': paragraph_num,
                                          'sentid': sentence_num,
                                          'sentence': sentence.to_string(False),
                                          'has_citations': sentence.has_citations,
                                          'citations': sentence.get_xref_pmid_set(self.rid_pmid_map, True)}
                    sentence_meta_list.append(sentence_meta_item)
        return article_meta_list, section_meta_list, sentence_meta_list

    def build_rid_pmid_map_all_level(self):
        article_groups = self.get_xref_pmid_set(True)
        section_groups = []
        paragraph_groups = []
        sentence_groups = []
        for section in self.section_list:
            section_groups.append(section.get_xref_pmid_set(self.rid_pmid_map, True))
            for plist in section.paragraph_list:
                paragraph_groups.append(plist.get_xref_pmid_set(self.rid_pmid_map, True))
                for sentence in plist.sentence_list:
                    sentence_groups.append(sentence.get_xref_pmid_set(self.rid_pmid_map, True))

        return (article_groups, section_groups, paragraph_groups, sentence_groups)

    def get_rid_pmid_map(self, root):
        return self.rid_pmid_map

    def _get_rid_pmid_map(self, root):
        rid_pmid_map = {}
        pub_id_expr = './/pub-id[@pub-id-type="pmid"]'
        ref_list = self.get_ref_list_with_pmid(root)
        for ref in ref_list:
            ref_id = ref.get('id')
            pub_id_list = ref.xpath(pub_id_expr)
            if len(pub_id_list) > 1:
                print('exception: node ref expected to have only one pub-id')
            else:
                pmid = pub_id_list[0].text
                rid_pmid_map[ref_id] = pmid
        return rid_pmid_map

    def get_ref_list_with_pmid(self, tree_root):
        # expr='//back/ref-list/ref/element-citation/pub-id[@pub-id-type="pmid"]/../..'
        expr = '//back/ref-list/ref[.//pub-id[@pub-id-type="pmid"]]'
        ref_list = tree_root.xpath(expr)
        return ref_list

    def __str__(self):
        new_str = ''
        for section in self.section_list:
            new_str += str(section)
        return new_str

    def to_string(self, with_xref_group=True):
        str_rep = ''
        if not with_xref_group:
            for sec in self.section_list:
                str_rep += sec.to_string(with_xref_group)
            return str_rep
        else:
            return self.__str__()

    def get_xref_pmid_set(self, allow_cache):
        if allow_cache and self.xref_pmid_set:
            return self.xref_pmid_set
        article_pmid_set = set()
        for sec in self.section_list:
            article_pmid_set = article_pmid_set | sec.get_xref_pmid_set(self.rid_pmid_map, allow_cache)
        self.xref_pmid_set = article_pmid_set
        return article_pmid_set

    def get_pmid(self):
        return self.pmid

    def _get_pmid(self, root):
        pmid_xpath = './/article-meta/article-id[@pub-id-type="pmid"]'
        article_id_node = root.xpath(pmid_xpath)
        # if (len(article_id_node) > 1):
        #    raise Exception('an article should have only one PMID')
        try:
            article_id_node_frist = article_id_node[0]
            pmid = article_id_node_frist.text
        except:
            pmid = None
        return pmid

    def gen_sec_list(self, root):
        section_arr = []
        # sec may nested in another sec
        sec_xpath = './body/sec'
        sec_list = root.xpath(sec_xpath)
        for sec in sec_list:
            # print(sec.get('id'))
            section = Section(sec, self)
            section_arr.append(section)
        return section_arr
