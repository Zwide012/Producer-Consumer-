
import os
import random
from dataclasses import dataclass
import xml.etree.cElementTree as ET
from collections import deque
import threading
import time
import socket

# Server
HOST = "127.0.0.1"
PORT = 5000

os.makedirs("Students_xml", exist_ok=True)

# student class creation.


@dataclass
class ITStudent:
    studentID: str
    name: str
    programme: str
    courses: list
    marks: list


# Shared buffer
bufferMax = 10
buffer = deque()
mutex = threading.Lock()
empty = threading.Semaphore(bufferMax)
full = threading.Semaphore(0)


# Generate random students
def randomStudentData():
    firstName = ["Nandiso Shabalala", "Phathizwe Ndwandwe",
                 "Thando Shongwe", "Rachel Zwide", "Nomzamo Kunene"]
    programmes = ["BSc IT", "Computer Science", "Accounting"]
    coursesList = ["Data Structures", "Programming",
                   "Database", "Networking", "Calculus"]

    studentID = "".join(str(random.randint(0, 9)) for _ in range(8))
    name = random.choice(firstName)
    programme = random.choice(programmes)
    sumCourses = random.randint(3, 5)
    courses = random.sample(coursesList, sumCourses)
    marks = [random.randint(0, 100) for _ in range(sumCourses)]

    return ITStudent(studentID, name, programme, courses, marks)

# Student data into XML


def studentXmlFileWrap(student: ITStudent, filename: str):
    root = ET.Element("ITStudent")
    ET.SubElement(root, "StudentID").text = student.studentID
    ET.SubElement(root, "Name").text = student.name
    ET.SubElement(root, "Programme").text = student.programme
    courses_el = ET.SubElement(root, "Courses")

    for cour, mars in zip(student.courses, student.marks):
        cour_el = ET.SubElement(courses_el, "Courses")
        ET.SubElement(cour_el, "Title").text = cour
        ET.SubElement(cour_el, "Mark").text = str(mars)

    tree = ET.ElementTree(root)
    tree.write(os.path.join("Students_xml", filename),
               encoding='utf-8', xml_declaration=True)

# Student data from File


def studentXmlFileUnwrap(filename: str) -> ITStudent:
    tree = ET.parse(filename)
    root = tree.getroot()
    studentID = root.find("StudentID").text
    name = root.find("Name").text
    programme = root.find("Programme").text
    courses = []
    marks = []

    for cour in root.find("Courses").findall("Courses"):
        courses.append(cour.find("Title").text)
        marks.append(int(cour.find("Mark").text))

    return ITStudent(studentID, name, programme, courses, marks)

# Function for producer


def producer():
    for i in range(1, 21):
        empty.acquire()
        student = randomStudentData()
        fileInt = (i-1) % 10 + 1
        Stud_xml = f"student{fileInt}.xml"
        studentXmlFileWrap(student, Stud_xml)

        with mutex:
            buffer.append(fileInt)
            print(f"Producer Added {Stud_xml} to buffer.")

        socket_client(Stud_xml)
        full.release()


# Function for consumer
def consumer():
    consumed = 0
    while consumed < 20:
        full.acquire()
        with mutex:
            fileInt = buffer.popleft()
            empty.release()
            fileName = f"student{fileInt}.xml"
            path = os.path.join("Students_xml", fileName)

            if not os.path.exists(path):
                consumed = consumed + 1
                continue
            student = studentXmlFileUnwrap(path)

            if student.marks:
                avg = sum(student.marks) / len(student.marks)
            else:
                avg = 0
            status = "PASS" if avg >= 50 else "FAIL"

            print(f"\nConsumer {fileName} processed")
            print("ID:", student.studentID)
            print("Name:", student.name)
            print("Programme:", student.programme)
            for cour, mar in zip(student.courses, student.marks):
                print(f" {cour}: {mar}")
            print("Average:", round(avg, 2), "%")
            print("Result:", status)
            os.remove(path)
            consumed = consumed + 1

# Implementing Socket Programming


def socket_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print("Socket Server: Consumer is listening")
    while True:
        conn, addr = server.accept()
        data = conn.recv(1024).decode()
        if data == "STOP":
            conn.close()
            break
        print(f"Socket Server Received: {data}")
        conn.close()


def socket_client(message):
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((HOST, PORT))
        client.send(message.encode())
        client.close()
    except ConnectionRefusedError:
        print("Socket Client, Server not ready")


t1 = threading.Thread(target=producer, name="ProducerThread")
t2 = threading.Thread(target=consumer, name="ConsumerThread")

socket_thread = threading.Thread(target=socket_server, daemon=True)
socket_thread.start()
t1.start()
t2.start()
t1.join()
t2.join()

socket_client("STOP")
# print(f"Finished. produced= {produced_count},consumed = {consumed_count}")
