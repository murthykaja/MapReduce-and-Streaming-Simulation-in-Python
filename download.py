import requests
import shutil
def callme():
    url = "http://www3.cs.stonybrook.edu/~has/CSE545/a2/test_COVID-19_Hospital_Impact.csv"
    r = requests.get(url, verify=False,stream=True)
    if r.status_code!=200:
        print("Failure!!")
        exit()
    else:
        r.raw.decode_content = True
        with open("/Users/murthykaja/Desktop/test_COVID-19_Hospital_Impact.csv", 'wb') as f:
            shutil.copyfileobj(r.raw, f)
        print("Success")

if __name__ == '__main__':
    callme()