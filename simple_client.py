import requests
import io

url = 'http://localhost:5000/put_stamps'

infile = open('/tmp/input_test.avro','rb')
data = infile.read()
infile.close()

files = {
	 'test' : data
}

resp = requests.post( url, files=files )
