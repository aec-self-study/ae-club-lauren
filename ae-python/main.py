# Write a loop that uses while 
from math import remainder
from typing import TypedDict

def while_loop():
    
    # initialize 
    i = 0
    max = 10
    while i <= max:
        print(f"We are only at {i}, {max-i} left to go! ")
        i += 1
        
        
def loop_keys(dict):
    [print(v) for v in dict.values()]
    

def is_odd(number):
    
    rem = remainder(number, 2)
    if rem == 0:
        print("Even")
    else:
        print("Odd!")
       
def square_dict(dictionary: TypedDict):
    [print(i*i) for i in dictionary.values()]
     
        

if __name__ == "__main__":
    #while_loop()
    #loop_keys({"This":1, "Key":2})
    #is_odd(19)
    #cal_lookup = {'apple': 95, 'banana': 105, 'orange': 45}
    #square_dict(cal_lookup)