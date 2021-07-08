import sys
from unittest import skipIf

from django.test import TestCase

from django_clickhouse.compatibility import namedtuple


class NamedTupleTest(TestCase):
    def test_defaults(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[3])
        self.assertTupleEqual((1, 2, 3), tuple(TestTuple(1, b=2)))
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(1, 2, 4)))
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(a=1, b=2, c=4)))

    @skipIf(sys.version_info < (3, 7),
            "On python < 3.7 this error is not raised, as not given defaults are filled by None")
    def test_no_required_value(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[3])

        with self.assertRaises(TypeError):
            TestTuple(b=1, c=4)

    def test_duplicate_value(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[3])

        with self.assertRaises(TypeError):
            TestTuple(1, 2, 3, c=4)

    def test_different_defaults(self):
        # Test that 2 tuple type defaults don't affect each other
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[3])
        OtherTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[4])
        t1 = TestTuple(a=1, b=2)
        t2 = OtherTuple(a=3, b=4)
        self.assertTupleEqual((1, 2, 3), tuple(t1))
        self.assertTupleEqual((3, 4, 4), tuple(t2))

    def test_defaults_cache(self):
        # Test that 2 tuple instances don't affect each other's defaults
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'), defaults=[3])
        self.assertTupleEqual((1, 2, 4), tuple(TestTuple(a=1, b=2, c=4)))
        self.assertTupleEqual((1, 2, 3), tuple(TestTuple(a=1, b=2)))

    def test_equal(self):
        TestTuple = namedtuple('TestTuple', ('a', 'b', 'c'))
        t1 = TestTuple(1, 2, 3)
        t2 = TestTuple(1, 2, 3)
        self.assertEqual(t1, t2)
        self.assertEqual((1, 2, 3), t1)
