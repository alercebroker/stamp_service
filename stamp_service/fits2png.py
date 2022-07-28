import gzip
import io

import astropy.io.fits as fio
import matplotlib.pyplot as plt
import numpy as np
from scipy import ndimage


def _read_compressed_fits(compressed_fits_file):
    try:
        fits = io.BytesIO(compressed_fits_file)
        return fio.open(gzip.open(fits))[0]
    except OSError:
        fits = io.BytesIO(compressed_fits_file)
        return fio.open(fits)[0]


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
    is_diff = file_type != "difference"
    vmax, vmin = get_max(data, window) if is_diff else (data.max(), data.min())

    buf = io.BytesIO()

    fig = plt.figure()
    ax = fig.add_subplot()

    opts = dict(cmap='Greys_r', interpolation="nearest", vmin=vmin, vmax=vmax)
    try:
        data = ndimage.rotate(data, hdu.header['PA'])
        opts['origin'] = 'lower'
    except KeyError:
        opts['origin'] = 'upper'
    ax.imshow(data, **opts)

    ax.axis("off")
    fig.savefig(buf, format="png", bbox_inches='tight', transparent=True)
    ax.clear()
    fig.clear()
    plt.close(fig=fig)

    buf.seek(0)
    return buf.read()
