#!/usr/bin/python3
import re
import os
import sys
from lxml import etree

def open_file (path):
    with open(path) as f:
        tree= etree.parse(f)
    return tree

def get_inumber (child, index):
    for inode in child.findall("inode"):
        if inode.find("name").text:
            name = inode.find("name").text
            name = re.sub('-', ' ', name)
            name = name.split(".")[0]
            token= name.lower().split()
            for tk in token:
                inum = inode.find("id").text
                if tk in index.keys():
                    index[tk].append(inum)
                else:
                    index[tk] = [inum]
        else:
            pass
    return index

def posting_list (idict, op):
    root = etree.Element("index")
    for token in idict.keys():
        postings = etree.Element("postings")
        name = etree.Element("name")
        name.text = token
        postings.append(name)
        for inum in idict[token]:
            inumber = etree.Element("inumber")
            inumber.text = inum
            postings.append(inumber)
        root.append(postings)
    op = open(op, "wb")
    op = op.write(etree.tostring(root, pretty_print=True))
    return op 
        
def create_index (ip, op):
    file = open_file(ip)        
    idict = {}
    for child in file.getroot():
        if child.tag == 'INodeSection':
            get_inumber(child, idict)
             
    index = posting_list(idict, op)
    
#create_index ("/Users/phuongqn/Desktop/INF551/hw2/fsimage564.xml", "index.xml")

if __name__ == "__main__":
    
    if(len(sys.argv) != 3):
        print("retry")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        create_index (input_path, output_path)
        

