#Import Statements
import urllib.request
import csv
import argparse


class Queue:
    '''Class to create a queue'''
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server:
    '''Server class akin to the Printer from reading'''
    def __init__(self):
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = int(self.time_remaining) - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_pages()


class Request:
    '''Request class akin to Task from reading'''
    #Update class to take the time to process from the file
    def __init__(self, time, ttp):
        self.timestamp = int(time)
        self.pages = ttp

    def get_stamp(self):
        return self.timestamp

    def get_pages(self):
        return self.pages

    def wait_time(self, current_time):
        # When using Round Robin it creates a larger disparity between the seconds of each item,
        # which means their is no flow restrictions in delivery.
        # This leads to false reported times. Should only be the processing time, as wait time should be approaching 0.
        if current_time - int(self.pages) >= int(self.timestamp):
            return self.timestamp - current_time
        else:
            return int(self.pages)



def simulationOneServer(url):
    print("Here in 1")
    '''Part One - One Server'''
    lab_printer = Server()
    srv_queue = Queue()
    waiting_times = []

    #Get file contents
    response = urllib.request.urlopen(url)
    lines = [l.decode('utf-8') for l in response.readlines()]
    requests = csv.reader(lines)

    #Populate queue
    for request in requests:
        srv_queue.enqueue(Request(request[0], request[2]))

    #Pre-populate first task
    current_task = srv_queue.dequeue()

    for current_second in range(srv_queue.size()):
        if (not lab_printer.busy()) and (not srv_queue.is_empty()):
            #Get next task
            next_task = srv_queue.dequeue()
            #Calculate next_task - current_task time, append to list
            waiting_times.append(next_task.wait_time(current_task.get_stamp()))
            #Start next task
            lab_printer.start_next(next_task)
            current_task = next_task

        lab_printer.tick()
    #Find the average time
    average_wait = sum(waiting_times) / len(waiting_times)
    print(f"Average Wait time is {average_wait:.4f} with 1 Server")

def simulateManyServers(url, servers):
    '''Part Two - Load Balancer'''
    lab_printer = Server()
    srv_queue = Queue()
    waiting_times = []
    server_count = servers
    q_list = []
    counter = 0

    #Get file contents
    response = urllib.request.urlopen(url)
    lines = [l.decode('utf-8') for l in response.readlines()]
    requests = csv.reader(lines)

    #List Comprehension to generate a list of queues
    q_list = [Queue() for i in range(server_count)]


    #uses a round robin assignment, to assign items to queues.
    for request in requests:
        q_list[counter].enqueue(Request(request[0], request[2]))
        if counter < server_count-1:
            #increment to next queue, if at max, restart to zero
            counter += 1
        else:
            counter = 0


    #Process each queue
    average_wait = []
    for round_robin in range(server_count):
        #Prepopulate first task
        current_task = q_list[round_robin].dequeue()
        for current_second in range(q_list[round_robin].size()):
            if (not lab_printer.busy()) and (not q_list[round_robin].is_empty()):
                #Get next task
                next_task = q_list[round_robin].dequeue()
                #Calculate next_task - current_task time, append to list
                waiting_times.append(next_task.wait_time(current_task.get_stamp()+int(current_task.get_pages())))
                #Start next task
                lab_printer.start_next(next_task)
                current_task = next_task
            lab_printer.tick()
        #Calculate average time
        average_wait.append(sum(waiting_times) / len(waiting_times))

    #Calculate the average wait time
    total_time = 0
    for time in range(len(average_wait)):
        total_time += average_wait[time]
    total_avg_wait = total_time/servers
    print(f"Average Wait time is {total_avg_wait:.4f} with {servers} servers")

def main(url,servers):
    '''Main Function, determine which simulation to run'''
    print(f"Running main with URL = {url}...")
    if servers == None or servers == 1:
        simulationOneServer(url)
    elif servers <= 0:
        print("Servers should be greater than 0")
        exit()
    else:
        simulateManyServers(url, servers)


if __name__ == "__main__":
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="URL to the datafile", type=str, required=True)
    parser.add_argument("--servers", help="Integer Number of Servers", type=int, required=False)
    args = parser.parse_args()
    main(args.url, args.servers)
