import astropy.io.fits as fio
import matplotlib.pyplot as plt
import io
import gzip
import numpy as np

def get_max(compressed_fits_file, window):
    f = io.BytesIO(compressed_fits_file)
    gf = gzip.open(f)
    hdu = fio.open(gf)[0]

    x = hdu.shape[0]//2
    y = hdu.shape[1]//2
    n = window
    data = hdu.data
    center = data[np.arange(x-n,x+n),:]
    center = center[:,np.arange(y-n,y+n)]
    max_val = np.max(center)
    min_val = np.min(data) + 0.2*np.median(np.abs(data - np.median(data)))

    return max_val,min_val

def transform(compressed_fits_file, file_type, max_val,min_val):
    f = io.BytesIO(compressed_fits_file)
    gf = gzip.open(f)
    hdu = fio.open(gf)[0]

    data = hdu.data
    if file_type != "difference":
        data[data > max_val] = max_val
        data[data < min_val] = min_val

    plt.figure()
    plt.matshow(data,cmap='Greys_r',interpolation="nearest")
    plt.axis("off")
    buf = io.BytesIO()
    plt.savefig(buf,format="png", bbox_inches='tight', transparent=True)
    buf.seek(0)
    im = buf.read()
    plt.cla()
    plt.clf()
    plt.close()
    return im
