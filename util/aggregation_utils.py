import hashlib


def inner_product(list1: list, list2: list):
    """
    inner_product: Calculate the inner product of two lists

    Parameters
    ----------
    list1
    list2

    Returns
    -------

    """

    inner_product_of_lists = 0

    for a, b in zip(list1, list2):
        inner_product_of_lists += a * b

    return inner_product_of_lists


def update_cms(sketch, v, w):
    """
    update_cms: Update the Count-Min Sketch with the given vector v

    Parameters
    ----------
    sketch
    v
    w

    Returns
    -------

    """

    hash_val1 = hashlib.sha1(str(v).encode()).hexdigest()
    hash_val2 = hashlib.sha256(str(v).encode()).hexdigest()
    hash_val3 = hashlib.md5(str(v).encode()).hexdigest()
    sketch[0][int(hash_val1, 16) % w] += 1
    sketch[1][int(hash_val2, 16) % w] += 1
    sketch[2][int(hash_val3, 16) % w] += 1
