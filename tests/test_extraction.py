import unittest
from unittest.mock import Mock, patch
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from get_brewery_data import BreweryAPIClient, BreweryDataSaver


class TestBreweryAPIClient(unittest.TestCase):
    
    def test_client_initialization(self):
        client = BreweryAPIClient()
        self.assertIsNotNone(client.base_url)
        self.assertIn("openbrewerydb", client.base_url)
    
    @patch('get_brewery_data.requests.get')
    def test_get_brewery_data_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = [{"id": "1", "name": "Test Brewery"}]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        client = BreweryAPIClient()
        result = client.get_brewery_data(page=1, per_page=50)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Test Brewery")
    
    @patch('get_brewery_data.requests.get')
    def test_get_total_data_count(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"total": 8000}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        client = BreweryAPIClient()
        total = client.get_total_data_count()
        
        self.assertEqual(total, 8000)


class TestBreweryDataSaver(unittest.TestCase):
    
    def test_saver_initialization(self):
        saver = BreweryDataSaver(base_dir="./test_data")
        self.assertIsNotNone(saver.dir_path)
        self.assertIn("ingested_at_", saver.dir_path)
    
    @patch('get_brewery_data.open', create=True)
    @patch('get_brewery_data.json.dump')
    def test_save_brewery_data(self, mock_json_dump, mock_open):
        saver = BreweryDataSaver(base_dir="./test_data")
        test_data = [{"id": "1", "name": "Brewery"}]
        
        saver.save_brewery_data(test_data, page=1)
        
        mock_open.assert_called_once()
        mock_json_dump.assert_called_once()
    
    def test_directory_creation(self):
        import tempfile
        import shutil
        
        temp_dir = tempfile.mkdtemp()
        try:
            saver = BreweryDataSaver(base_dir=temp_dir)
            self.assertTrue(os.path.exists(saver.dir_path))
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
