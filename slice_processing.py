from math import ceil, sqrt


def process(seed, size=256):
    pixels = []
    histogram = [0] * 4096
    for _ in range(size * size):
        seed = (1103515245 * seed + 12345) & 0x7fffffff
        pixel = seed >> 19
        pixels.append(pixel)
        histogram[pixel] += 1
    cumulative = 0
    lo, hi = None, None
    for level, count in enumerate(histogram):
        cumulative += count
        if lo is None and cumulative >= ceil(len(pixels) * .02):
            lo = level
        if cumulative >= ceil(len(pixels) * .98):
            hi = level
            break
    width = hi - lo
    kept = total = squares = 0
    for pixel in pixels:
        if lo <= pixel <= hi:
            value = (8190 * (pixel - lo) + width) // (2 * width) if width else 0
            kept += 1
            total += value
            squares += value * value
    mean = total / kept
    return dict(p_lo=lo, p_hi=hi, removed=len(pixels)-kept,
                mean=mean, std=sqrt(max(0, squares/kept-mean*mean)))
