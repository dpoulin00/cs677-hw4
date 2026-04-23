

To run the program with 6 nodes, run "python main.py 6" in the console.
To run the program with any other number of nodes, just replace the 6 with the desired number of nodes.
Once you do so, the program will generate a random network with that many nodes and start running.

The program will run for 10,000 seconds (about 3 hours). Then, it will terminate. To stop it prematurely,
close the terminal.

To run the tests, run "python test.py".
To choose which test, in the "if __name__ == '__main__'" block,
change the value assigned to test_num to the value corresponding to the desired test.
Note that we ran all tests, even though only one is currently set to run.

To run the performance tests, run performance_testing.py. Note that this file by default will pull from
the existing CSVs in the "performance test logs" directory. If you want to re-generate node_0_timestamps.csv, you can
do so by deleting "node_0_timestamps.csv" and running the file. Be warned that it takes over an hour to process this
many requests.


Dependencies
pandas
numpy

