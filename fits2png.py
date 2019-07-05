import io
import numpy
from astropy.io import fits
import gzip
import img_scale
from PIL import Image

def transform(compressed_fits_file):

	# Parameters
	sig_fract = 5.0
	per_fract = 5.0-2
	max_iter = 20
	min_val = 0.0
	factor = 1.0
	non_linear_fact = 0.005

	# Read red image
	bytes_fits = gzip.open(compressed_fits_file,'rb').read()
	fits_file = io.BytesIO(bytes_fits)
	hdul = fits.open(fits_file)
	img_data = hdul[0].data
	hdul.close()

	width=img_data.shape[0]
	height=img_data.shape[1]
	img_data = numpy.array(img_data, dtype=float)
	sky, num_iter = img_scale.sky_median_sig_clip(img_data, sig_fract, per_fract, max_iter, low_cut=False, high_cut=True)
	img_data = img_data - sky


	# Apply scaling relations
	data = factor * img_scale.histeq(img_data, scale_min = min_val )

	#GRAY image with SciPy
	data /=  data.max(axis=0)
	data *= (255.0/data.max())
	data = data.astype(numpy.uint8)

	data_array = numpy.array( numpy.ravel(data) )
	data_array.astype(numpy.int)
	use_image = Image.new('L', size=(height, width))
	use_image.putdata(data_array)

	binary_file = io.BytesIO()
	use_image.save(binary_file,format='png')

	return binary_file
