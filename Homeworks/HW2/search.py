import sys
import re
from lxml import etree

def get_metadata(inumber, path):
    tree = etree.parse(path)
    metadata = {}
    xpath = '//INodeSection/inode[id="' + inumber + '"]'
    for nodes in tree.xpath(xpath):
        for c in nodes:
            if c.tag == "id":
                metadata["id"] = c.text
            elif c.tag == "type":
                metadata["type"] = c.text
            elif c.tag == "mtime":
                metadata["mtime"] = c.text
            elif c.tag == "blocks":
                blocks = []
                for block in c:
                    for b in block:
                        if b.tag == "id":
                            blocks.append(b.text)             
                metadata['blocks'] = blocks
    return metadata

def get_inumbers(index_file, query):

    tree = etree.parse(index_file)
    candidate = set()
    
    query = query.split(".")[0]
    query = re.sub('-', ' ', query)
    queries = query.lower().split()

    for q in queries:
        path = '//postings[name="' + q + '"]'
        inum = set()
        for postings in tree.xpath(path):
            for p in postings:
                if p.tag == 'inumber':
                    inum.add(p.text)
        if len(candidate)== 0:
            candidate = candidate.union(inum)
        else:
            candidate = candidate.intersection(inum)

    candidates = list(candidate)
    candidates.sort()
                    
    return candidates


def build_directories (fsimage):
  
    directories = {}
    tree = etree.parse(fsimage)
    xpath =  '//INodeDirectorySection/directory'
    for directory in tree.xpath(xpath):
        parent = None
        child = []
        for d in directory:
            if str(d.tag) == 'parent':
                parent = d.text
            elif str(d.tag) == 'child':
                child.append(d.text)

        if parent is not None:
            for c in child:
                directories[c] = parent 
   
    return directories
            
def get_path(inumber, directory, fsimage):
    tree = etree.parse(fsimage)
    path = '//INodeSection/inode[id="' + inumber + '"]'
    filename = ""
    if inumber == None or not (inumber in directory):
        return ""
    else:
        for nodes in tree.xpath(path):
            for child in nodes:
                if child.tag == "name":
                    filename = child.text 
    new_path = get_path(directory[inumber], directory, fsimage)
    
    return new_path + "/" + filename   

if __name__ == "__main__":
    
    if(len(sys.argv) == 4):
        image_path = sys.argv[1]
        index_path = sys.argv[2]
        query= sys.argv[3]
        
        directories = build_directories (image_path)
        results = get_inumbers (index_path, query)
        
        for r in results:
            print (get_path(r, directories, image_path))
            print (str(get_metadata (r, image_path)))
        
    else:
        print("retry")
        exit()
