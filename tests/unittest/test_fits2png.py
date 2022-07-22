import io
import unittest
from unittest import mock
import numpy as np
from stamp_service import fits2png


class TestGetMaxValueRange(unittest.TestCase):
    def test_get_max_only_gets_maximum_inside_window(self):
        xsize, ysize = 10, 10
        data = np.zeros((xsize, ysize))
        window = 1
        max_in, max_out = 1, 5  # Higher outside window
        data[xsize // 2, ysize // 2] = max_in
        data[0, 0] = max_out

        vmax, _ = fits2png.get_max(data, window)
        self.assertEqual(max_in, vmax)

    def test_get_max_gets_minimum_from_full_data(self):
        xsize, ysize = 10, 10
        np.random.seed(616)
        data = np.random.random((xsize, ysize))
        window = 1
        expected = np.min(data) + .2 * np.median(np.abs(data - np.median(data)))

        _, vmin = fits2png.get_max(data, window)
        self.assertEqual(expected, vmin)


@mock.patch('stamp_service.fits2png.plt')
@mock.patch('stamp_service.fits2png.get_max')
class TestFITS2PNGTransform(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = np.zeros((10, 10))

    @mock.patch('stamp_service.fits2png.fio')
    @mock.patch('stamp_service.fits2png.gzip')
    def test_opening_of_gzipped_fits(self, mock_gzip, mock_fio, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_fio.open.return_value[0].data = self.data

        fits2png.transform(b'', '', 2)
        mock_fio.open.assert_called()
        mock_gzip.open.assert_called()

    @mock.patch('stamp_service.fits2png.fio')
    @mock.patch('stamp_service.fits2png.gzip')
    def test_opening_of_not_gzipped_fits(self, mock_gzip, mock_fio, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_fio.open.return_value[0].data = self.data
        mock_gzip.side_effect = IOError()

        fits2png.transform(b'', '', 2)
        mock_fio.open.assert_called()
        mock_gzip.open.assert_called()

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_when_using_difference_stamp_do_not_change_min_max(self, mock_read, mock_max, mock_plt):
        mock_read.return_value.data = self.data

        fits2png.transform(b'', 'difference', 2)
        mock_max.assert_not_called()

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_when_using_stamp_not_difference_change_min_max(self, mock_read, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_read.return_value.data = self.data

        fits2png.transform(b'', '', 2)
        mock_max.assert_called()

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_pyplot_removes_axis_from_figure(self, mock_read, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_read.return_value.data = self.data

        fits2png.transform(b'', '', 2)
        mock_plt.figure.return_value.add_subplot.return_value.axis.assert_called_with('off')

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_pyplot_saves_image_as_png_in_bytes_buffer(self, mock_read, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_read.return_value.data = self.data

        fits2png.transform(b'', '', 2)
        args = mock_plt.figure.return_value.savefig.call_args
        self.assertIsInstance(args.args[0], io.BytesIO)
        self.assertEqual('png', args.kwargs['format'])

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_figure_is_closed(self, mock_read, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_read.return_value.data = self.data

        fits2png.transform(b'', '', 2)
        mock_plt.close.assert_called()

    @mock.patch('stamp_service.fits2png._read_compressed_fits')
    def test_output_is_a_bytes_object(self, mock_read, mock_max, mock_plt):
        mock_max.return_value = 1, 0
        mock_read.return_value.data = self.data

        out = fits2png.transform(b'', '', 2)
        self.assertEqual(b'', out)
