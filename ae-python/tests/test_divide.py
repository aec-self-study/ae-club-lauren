import unittest
from calc import Calc

class TestDivide(unittest.TestCase):
    
    
    
    def test_cant_divide_by_zero(self):
        arg_ints = [20, 0]
        calculator = Calc(*arg_ints)
        div_result = calculator.divide()
        self.assertEqual(div_result, 0)

   

if __name__ == "__main__":
    unittest.main()