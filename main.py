from data_model import generatedata, generateFinanceData
import sys

def main():
    print("Script name:", sys.argv[0])
    records = int(sys.argv[1])
    try:
        generatedata(numofrecords= records)
        print(f"created file with {records} records")
        generateFinanceData()
    except:
        print("error in processing")
    
if __name__ == '__main__':
    main()