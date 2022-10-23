import argparse

from numpy import diff


class Calc:
     def __init__(self, *args):
        if len(args) > 2:
             self.ints_to_calc = args
        if len(args) == 2:
             self.int_1 = args[0]
             self.int_2 = args[1]
             
     def subtract(self):
        if len(self.ints_to_calc) > 2:
             raise Exception("Invalid number of arguments. Maximum of 2 arguments allowed")
        
        sub = self.int_1 - self.int_2
        if sub < 0:
            sub = 0
        return sub
    
     def divide(self):
        if len(self.ints_to_calc) > 2:
             raise Exception("Invalid number of arguments. Maximum of 2 arguments allowed")
        
        if self.int_2 == 0:
            print("Cannot divide by zero!")
            return 0
        else:
            div = self.int_1 / self.int_2
            return div
            
    


parser = argparse.ArgumentParser(description="A command line calculator.")

subparsers = parser.add_subparsers(help = "sub-command help", dest="command")

add = subparsers.add_parser("add", help="add integers")
add.add_argument("ints_to_sum", nargs="*", type=int)

sub = subparsers.add_parser("sub", help="subtract integers")
sub.add_argument("ints_to_sub", nargs=2, type=int)

multiply = subparsers.add_parser("multiply", help="multiply integers")
multiply.add_argument("ints_to_multiply", nargs="*", type=int)

divide = subparsers.add_parser("divide", help="divide integers")
divide.add_argument("ints_to_divide", nargs="*", type=int)


if __name__ == "__main__":
    args = parser.parse_args()
    


    # if args.command == "add":
    #     our_sum = sum(args.ints_to_sum)
    #     print(f"The sum of values is: {our_sum}")
        
    if args.command == "sub":
        calculator = Calc(*args.ints_to_sub)
        our_sub = calculator.subtract()
        print(f"The subtracted result is: {our_sub}")
        
    # if args.command == "multiply":
    #     our_mult = args.ints_to_multiply[0]*args.ints_to_multiply[1]
    #     print(f"The multiplied result is: {our_mult}")
        
    if args.command == "divide":
        calculator = Calc(*args.ints_to_divide)
        our_div = calculator.divide()
        print(f"The divided result is: {our_div}")