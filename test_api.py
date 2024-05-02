from datetime import datetime

import responses
import unittest
import API


class TestAPI(unittest.TestCase):

    @responses.activate
    def test_get_users_should_make_request(self):
        # Mock the API response
        responses.add(responses.GET,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+user_id__c,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c+FROM+user__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.get_users()

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+user_id__c,first_name__c,last_name__c,email__c,telephone__c,birthday__c,country__c,state__c,city__c,zip__c,street__c,house_number__c,company_email__c,company_id__c,source__c,user_role__c,invoice__c+FROM+user__c')

    @responses.activate
    def test_add_user_should_send_correct_request(self):
        # Mock the API response
        responses.add(responses.POST,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/user__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.add_user("test_id", "test_first_name", "test_last_name", "test_email", "test_telephone",
                              "test_birthday", "test_country", "test_state", "test_city", "test_zip", "test_street",
                              "test_house_number", "test_company_email", "test_company_id", "test_source",
                              "test_user_role", "test_invoice")

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/user__c')

        # Check the request body
        expected_body = f'''<user__c>
        <user_id__c>test_id</user_id__c>
        <first_name__c>test_first_name</first_name__c>
        <last_name__c>test_last_name</last_name__c>
        <email__c>test_email</email__c>
        <telephone__c>test_telephone</telephone__c>
        <birthday__c>test_birthday</birthday__c>
        <country__c>test_country</country__c>
        <state__c>test_state</state__c>
        <city__c>test_city</city__c>
        <zip__c>test_zip</zip__c>
        <street__c>test_street</street__c>
        <house_number__c>test_house_number</house_number__c>
        <company_email__c>test_company_email</company_email__c>
        <company_id__c>test_company_id</company_id__c>
        <source__c>test_source</source__c>
        <user_role__c>test_user_role</user_role__c>
        <invoice__c>test_invoice</invoice__c>
    </user__c>'''

        self.assertEqual(responses.calls[0].request.body.strip(), expected_body)

    @responses.activate
    def test_add_user_should_return_error_when_missing_id(self):
        with self.assertRaises(ValueError):
            API.add_user("", "test_first_name", "test_last_name", "test_email", "test_telephone", "test_birthday",
                         "test_country", "test_state", "test_city", "test_zip", "test_street", "test_house_number",
                         "test_company_email", "test_company_id", "test_source", "test_user_role")

    @responses.activate
    def test_add_user_should_return_error_when_missing_first_name(self):
        with self.assertRaises(ValueError):
            API.add_user("test_id", "", "test_last_name", "test_email", "test_telephone", "test_birthday",
                         "test_country", "test_state", "test_city", "test_zip", "test_street", "test_house_number",
                         "test_company_email", "test_company_id", "test_source", "test_user_role", "test_invoice")

    @responses.activate
    def test_add_user_should_return_error_when_missing_last_name(self):
        with self.assertRaises(ValueError):
            API.add_user("test_id", "test_first_name", "", "test_email", "test_telephone", "test_birthday",
                         "test_country", "test_state", "test_city", "test_zip", "test_street", "test_house_number",
                         "test_company_email", "test_company_id", "test_source", "test_user_role", "test_invoice")

    def test_add_user_should_return_error_when_missing_email(self):
        with self.assertRaises(ValueError):
            API.add_user("test_id", "test_first_name", "test_last_name", "", "test_telephone", "test_birthday",
                         "test_country", "test_state", "test_city", "test_zip", "test_street", "test_house_number",
                         "test_company_email", "test_company_id", "test_source", "test_user_role", "test_invoice")

    @responses.activate
    def test_get_companies_should_make_request(self):
        # Mock the API response
        responses.add(responses.GET,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+id__c,name__c,email__c,telephone__c,country__c,state__c,city__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.get_companies()

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+id__c,name__c,email__c,telephone__c,country__c,state__c,city__c,street__c,house_number__c,type__c,invoice__c+FROM+Company__c')

    @responses.activate
    def test_add_company_should_send_correct_request(self):
        # Mock the API response
        responses.add(responses.POST,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/Company__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.add_company("test_id", "test_name", "test_email", "test_telephone", "test_country", "test_state", "test_city", "test_zip", "test_street", "test_house_number", "test_type", "test_invoice")

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/Company__c')

        # Check the request body
        expected_body = f'''<Company__c>
            <id__c>test_id</id__c>
            <name>test_name</name>
            <email__c>test_email</email__c>
            <telephone__c>test_telephone</telephone__c>
            <country__c>test_country</country__c>
            <state__c>test_state</state__c>
            <city__c>test_city</city__c>
            <zip__c>test_zip</zip__c>
            <street__c>test_street</street__c>
            <house_number__c>test_house_number</house_number__c>
            <type__c>test_type</type__c>
            <invoice__c>test_invoice</invoice__c>
        </Company__c>'''

        self.assertEqual(responses.calls[0].request.body.strip(), expected_body)

    @responses.activate
    def test_add_company_should_return_error_when_missing_id(self):
        with self.assertRaises(ValueError):
            API.add_company("", "test_name", "test_email", "test_telephone", "test_country", "test_state", "test_city",
                            "test_zip", "test_street", "test_house_number", "test_type", "test_invoice")

    @responses.activate
    def test_add_company_should_return_error_when_missing_name(self):
        with self.assertRaises(ValueError):
            API.add_company("test_id", "", "test_email", "test_telephone", "test_country", "test_state", "test_city",
                            "test_zip", "test_street", "test_house_number", "test_type", "test_invoice")

    @responses.activate
    def test_company_should_return_error_when_missing_email(self):
        with self.assertRaises(ValueError):
            API.add_company("test_id", "test_name", "", "test_telephone", "test_country", "test_state", "test_city",
                            "test_zip", "test_street", "test_house_number", "test_type", "test_invoice")

    @responses.activate
    def test_get_talk_should_make_request(self):
        # Mock the API response
        responses.add(responses.GET,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+Id,Name,Date__c+FROM+Talk__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.get_talk()

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+Id,Name,Date__c+FROM+Talk__c')

    @responses.activate
    def test_add_talk_should_send_correct_request(self):
        # Mock the API response
        responses.add(responses.POST,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/event__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.add_talk("test_id", "test_date", "12:30", "13:30", "test_user_id", "test_available_seats", "test_description")

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/event__c')

        # Check the request body
        expected_body = f'''<event__c>
            <id__c>test_id</id__c>
            <date__c>test_date</date__c>
            <start_time__c>{datetime.strptime("12:30", '%H:%M').strftime('%H:%M:%S')}</start_time__c>
            <end_time__c>{datetime.strptime("13:30", '%H:%M').strftime('%H:%M:%S')}</end_time__c>
            <user_id__c>test_user_id</user_id__c>
            <available_seats__c>test_available_seats</available_seats__c>
            <description__c>test_description</description__c>
        </event__c>'''

        self.assertEqual(responses.calls[0].request.body.strip(), expected_body)

    @responses.activate
    def test_add_talk_should_return_error_when_missing_date(self):
        with self.assertRaises(ValueError):
            API.add_talk("test_id", "", "12:30", "13:30", "test_user_id", "test_available_seats", "test_description")

    @responses.activate
    def test_add_talk_should_return_error_when_missing_start_time(self):
        with self.assertRaises(ValueError):
            API.add_talk("test_id", "test_date", "", "13:30", "test_user_id", "test_available_seats", "test_description")

    @responses.activate
    def test_add_talk_should_return_error_when_missing_end_time(self):
        with self.assertRaises(ValueError):
            API.add_talk("test_id", "test_date", "12:30", "", "test_user_id", "test_available_seats", "test_description")



    @responses.activate
    def test_get_attendance_should_make_request(self):
        # Mock the API response
        responses.add(responses.GET,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+Id,Name,Talk__c,Portal_user__c+FROM+talkAttendance__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.get_attendance()

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/query?q=SELECT+Id,Name,Talk__c,Portal_user__c+FROM+talkAttendance__c')

    @responses.activate
    def test_add_attendance_should_send_correct_request(self):
        # Mock the API response
        responses.add(responses.POST,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/talkAttendance__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.add_attendance("test_user", "test_talk")

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/talkAttendance__c')

        # Check the request body
        expected_body = f'''<TalkAttendance__c>
            <Portal_user__c>test_user</Portal_user__c>
            <Talk__c>test_talk</Talk__c>
        </TalkAttendance__c>'''

        self.assertEqual(responses.calls[0].request.body.strip(), expected_body)

    @responses.activate
    def test_add_product_should_send_correct_request(self):
        # Mock the API response
        responses.add(responses.POST,
                      'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/product__c',
                      json={'key': 'value'}, status=200)

        # Call the function
        result = API.add_product("test_product")

        # Check that the request was made with the correct parameters
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url,
                         'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com/services/data/v60.0/sobjects/product__c')

        # Check the request body
        expected_body = f'''<product__c>
            <Name>test_product</Name>
        </product__c>'''

        self.assertEqual(responses.calls[0].request.body.strip(), expected_body)


if __name__ == '__main__':
    unittest.main()
