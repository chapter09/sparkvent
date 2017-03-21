from mock import patch
import sparkvent.config
import sparkvent.client


@patch('sparkvent.client')
@patch('sparkvent.config')
def test(MockClass1, MockClass2):
    sparkvent.client()
    sparkvent.config()
    assert MockClass2 is sparkvent.client
    assert MockClass1 is sparkvent.config


test()