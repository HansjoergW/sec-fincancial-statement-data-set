import secfsdstools.x_examples.example_collectors as collect
import secfsdstools.x_examples.example_presenter as presenter
import secfsdstools.x_examples.example_indexsearch as indexsearch
import secfsdstools.x_examples.example_companyindexreader as companyindex


def test_collector():
    collect.run()


def test_presenter():
    presenter.run()


def test_indexsearch():
    indexsearch.run()


def test_companyindex():
    companyindex.run()

