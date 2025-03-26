This module contains common classes used only in test. For example, it contains a SparkTest class that creates a common
SparkContext used by all the tests and avoids creating a context for each test. This helps running the tests faster
by avoiding the construction of multiple SparkContexts.