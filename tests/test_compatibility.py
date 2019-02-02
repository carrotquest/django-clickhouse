from unittest import TestCase

from django_clickhouse.compatibility import namedtuple


class NamedTupleTest(TestCase):
    def test_defaults(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults={'c': 3})
        self.assertTupleEqual((1, 2, 3), tuple(TestTuple(1, b=2)))
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(1, 2, 4)))
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(a=1, b=2, c=4)))

    def test_exceptions(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults={'c': 3})
        with self.assertRaises(TypeError):
            TestTuple(b=1, c=4)

        with self.assertRaises(TypeError):
            TestTuple(1, 2, 3, c=4)

    def test_different_defaults(self):
        # Test that 2 tuple type defaults don't affect each other
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults={'c': 3})
        OtherTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults={'c': 4})
        t1 = TestTuple(a=1, b=2)
        t2 = OtherTuple(a=3, b=4)
        self.assertTupleEqual((1, 2, 3), tuple(t1))
        self.assertTupleEqual((3, 4, 4), tuple(t2))

    def test_defaults_cache(self):
        # Test that 2 tuple instances don't affect each other's defaults
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults={'c': 3})
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(a=1, b=2, c=4)))
        self.assertTupleEqual((1, 2, 3), tuple(TestTuple(a=1, b=2)))


