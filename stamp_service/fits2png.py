import astropy.io.fits as fio
import matplotlib.pyplot as plt
import io
import gzip
import numpy as np


def _read_compressed_fits(compressed_fits_file):
    fits = io.BytesIO(compressed_fits_file)
    return fio.open(gzip.open(fits))[0]


def get_max(data, window):
    x = data.shape[0] // 2
    y = data.shape[1] // 2
    center = data[np.arange(x - window, x + window), :]
    center = center[:, np.arange(y - window, y + window)]
    max_val = np.max(center)
    min_val = np.min(data) + 0.2 * np.median(np.abs(data - np.median(data)))

    return max_val, min_val


def transform(compressed_fits_file, file_type, window):
    hdu = _read_compressed_fits(compressed_fits_file)

    data = hdu.data
    if file_type != "difference":
        max_val, min_val = get_max(data, window)
        data[data > max_val] = max_val
        data[data < min_val] = min_val

    plt.figure()
    plt.matshow(data, cmap='Greys_r', interpolation="nearest")
    plt.axis("off")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches='tight', transparent=True)
    buf.seek(0)
    im = buf.read()
    plt.cla()
    plt.clf()
    plt.close()
    return im
