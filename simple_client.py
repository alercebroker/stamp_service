import requests
import io

#PUT AVRO
#url = 'http://stamps.alerce.online:8087/put_avro'
#
#params = {
#	 'oid' : 'ZTF19aafmxhi',
#	 'candid' : 900171215015010003
#}
#
#infile = open('/tmp/900171215015010003.avro','rb')
#data = infile.read()
#infile.close()
#
#files = {'avro' : data}
#
#resp = requests.post( url, files=files, params=params  )

#GET STAMP
url = 'http://alerce.reuna.cl:8087/get_stamp'

params ={
	  'oid' : 'ZTF18aajhbfp',
	  'candid' : 821270966115015004,
	  'type': 'science',
	  'format': 'png'
}

resp = requests.post( url, params=params )
outfile = open('{}.{}'.format(params['candid'],params['format']),'wb')
outfile.write(resp.content)
outfile.close()

#GET AVRO
#url = 'http://stamps.alerce.online:8087/get_avro'
#
#params = {
#	 'oid' : 'ZTF19aafmxhi',
#	 'candid' : 900171215015010003
#}
#
#resp = requests.post( url, files=files, params=params  )
#outfile = open('900171215015010003.avro','wb')
#outfile.write(resp.content)
#outfile.close()
