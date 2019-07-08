import astropy.io.fits as fio
import matplotlib.pyplot as plt
import io
import gzip 

def transform(compressed_fits_file):
    f = io.BytesIO(compressed_fits_file)
    gf = gzip.open(f)
    hdu = fio.open(gf)[0]
    plt.figure()
    plt.matshow(-hdu.data,cmap='Greys',interpolation="bilinear")
    plt.axis("off")
    buf = io.BytesIO()
    plt.savefig(buf,format="png", bbox_inches='tight', transparent=True)
    buf.seek(0)
    im = buf.read()
    return im
