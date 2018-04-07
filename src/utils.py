
def process(keyArr):
    key, arr = keyArr
    return (key, sum(arr) if isinstance(arr, list) else arr)

def addList(a,b):

    tmp = []
    if isinstance(a,list):
        tmp = a
        tmp.append(b)
        return tmp
    else:
        return [a,b]