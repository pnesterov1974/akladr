from datetime import datetime

def zero_fill(in_int: int, digits=6):
    if in_int > 0:
        ls = str(in_int)
        delta = digits - len(ls)
        if delta > 0:
            mask_str = '0' * (digits - len(ls))
        else:
            mask_str = ''
        return ''.join([mask_str, ls])
    else:
        return ''
    
def timeit(wrapped):
    t0 = datetime.now()
    f = wrapped()
    t1 = datetime.now()
    td = t1-t0
    print(td)
    return f

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass