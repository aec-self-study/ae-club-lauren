import unittest
from calc import Calc


class TestSubtract(unittest.TestCase):
    
    
    
    def test_subtract(self):
        arg_ints = [20, 5]
        calculator = Calc(*arg_ints)
        sub_result = calculator.subtract()
        self.assertEqual(sub_result, 15)

    def test_cant_go_below_zero(self):
        arg_ints = [5, 20]
        calculator = Calc(*arg_ints)
        sub_result = calculator.subtract()
        self.assertEqual(sub_result, 0)   
        
    def test_limit_args(self):
        arg_ints = [3, 4, 8, 9]
        
        with self.assertRaises(Exception) as cm:
            calculator = Calc(*arg_ints)
            div_result = calculator.divide()
        self.assertEqual(str(cm.exception), "Invalid number of arguments. Maximum of 2 arguments allowed")
        

if __name__ == "__main__":
    unittest.main()