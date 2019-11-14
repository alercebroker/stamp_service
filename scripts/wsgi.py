import sys
sys.path.append("..")
from stamp_service import application

if __name__=="__main__":
    application.run(debug=True)
