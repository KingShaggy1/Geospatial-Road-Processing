import getopt, sys
from testcasesimagelibrary import TestCasesImageLibrary


if __name__ == '__main__':

    f = sys.argv

    if len(f) != 5:
        print("ERROR: Incorrect number of arguments")
        sys.exit(-1)
    
    #print('Number of arguments:', len(f), 'arguments.')
    #print('Argument List:', str(f))

    esImageLibrary= f[1]
    esImageLibraryPort= int(f[2])
    esAnalytics= f[3]
    esAnalyticsPort= int(f[4])

    print('Image:', esImageLibrary, ':', esImageLibraryPort, ' Analytics:', esAnalytics, ':', esAnalyticsPort, '\n', sep="")
    print("Started")

    #print("Setting up timezone and Calendar")
    #Time zone stuff here

    tc = TestCasesImageLibrary(esImageLibrary, esImageLibraryPort, esAnalytics, esAnalyticsPort)
    
    try:
        tc.setup()
        tc.testComputeImageCoverage()
        
        #print('Operation Completed')

    except:
        print(sys.exc_info())
        sys.exit()

