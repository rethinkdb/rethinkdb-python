import pytest
from copy import deepcopy

from tests.helpers import IntegrationTestCaseBase


@pytest.mark.integration
class TestDataWrite(IntegrationTestCaseBase):
    def setup_method(self):
        super(TestDataWrite, self).setup_method()
        self.table_name = 'test_data_write'
        self.r.table_create(self.table_name).run(self.conn)

        self.insert_data = {
            'id': 1,
            'name': 'Captain America',
            'real_name': 'Steven Rogers',
            'universe': 'Earth-616'
        }

        self.expected_insert_response = {
            'deleted': 0,
            'errors': 0,
            'inserted': 1,
            'replaced': 0,
            'skipped': 0,
            'unchanged': 0,
        }

        self.expected_update_response = {
            'deleted': 0,
            'errors': 0,
            'inserted': 0,
            'replaced': 1,
            'skipped': 0,
            'unchanged': 0,
        }

        self.expected_replace_response = {
            'deleted': 0,
            'errors': 0,
            'inserted': 0,
            'replaced': 1,
            'skipped': 0,
            'unchanged': 0,
        }

        self.expected_delete_response = {
            'deleted': 1,
            'errors': 0,
            'inserted': 0,
            'replaced': 0,
            'skipped': 0,
            'unchanged': 0,
        }

    def test_insert(self):
        response = self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        assert response == self.expected_insert_response

    def test_insert_multiple(self):
        self.expected_insert_response['inserted'] = 2

        response = self.r.table(self.table_name).insert([
            self.insert_data,
            {
                'id': 2,
                'name': 'Iron Man',
                'real_name': 'Anthony Edward Stark',
                'universe': 'Earth-616'
            }
        ]).run(self.conn)

        assert response == self.expected_insert_response

    def test_insert_durability_soft(self):
        response = self.r.table(self.table_name).insert(self.insert_data, durability='soft').run(self.conn)

        assert response == self.expected_insert_response

    def test_insert_durability_hard(self):
        response = self.r.table(self.table_name).insert(self.insert_data, durability='hard').run(self.conn)

        assert response == self.expected_insert_response

    def test_insert_return_changes(self):
        self.expected_insert_response['changes'] = [{
            'old_val': None,
            'new_val': self.insert_data
        }]

        response = self.r.table(self.table_name).insert(self.insert_data, return_changes=True).run(self.conn)

        assert response == self.expected_insert_response

    def test_insert_conflict_error(self):
        initial_insert_response = self.r.table(self.table_name).insert(self.insert_data).run(self.conn)
        response = self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        assert initial_insert_response == self.expected_insert_response
        assert response['inserted'] == 0
        assert response['errors'] == 1
        assert 'Duplicate primary key' in response['first_error']

    def test_insert_conflict_replace(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'Iron Man'})
        response = self.r.table(self.table_name).insert(self.insert_data, conflict='replace').run(self.conn)
        document = self.r.table(self.table_name).get(1).run(self.conn)

        assert response['inserted'] == 0
        assert response['replaced'] == 1
        assert document == self.insert_data

    def test_insert_conflict_update(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'birthday': '1918-07-04'})
        response = self.r.table(self.table_name).insert(self.insert_data, conflict='update').run(self.conn)
        document = self.r.table(self.table_name).get(1).run(self.conn)

        assert response['inserted'] == 0
        assert response['replaced'] == 1
        assert document == self.insert_data

    def test_update_on_table(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).update({
            'name': self.insert_data['name'],
            'birthday': '1918-07-04'
        }).run(self.conn)

        assert response == self.expected_update_response

    def test_update_by_id(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).update({
            'birthday': '1918-07-04'
        }).run(self.conn)

        assert response == self.expected_update_response

    def test_update_by_filter_result(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).filter({'id': self.insert_data['id']}).update({
            'birthday': '1918-07-04'
        }).run(self.conn)

        assert response == self.expected_update_response

    def test_update_durability_soft(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).update({
            'birthday': '1918-07-04'
        }, durability='soft').run(self.conn)

        assert response == self.expected_update_response

    def test_update_durability_hard(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).update({
            'birthday': '1918-07-04'
        }, durability='hard').run(self.conn)

        assert response == self.expected_update_response

    def test_update_return_changes(self):
        update_data = deepcopy(self.insert_data)
        update_data.update({
            'birthday': '1918-07-04'
        })

        self.expected_update_response['changes'] = [{
            'old_val': self.insert_data,
            'new_val': update_data
        }]

        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).update({
            'birthday': '1918-07-04'
        }, return_changes=True).run(self.conn)

        assert response == self.expected_update_response

    def test_update_non_atomic(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).update({
            'birthday': '1918-07-04'
        }, non_atomic=True).run(self.conn)

        assert response == self.expected_update_response

    def test_replace(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'The Great {name}'.format(name=self.insert_data['name'])})
        response = self.r.table(self.table_name).get(self.insert_data['id']).replace(
            self.insert_data
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_replace_by_filter_result(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'The Great {name}'.format(name=self.insert_data['name'])})
        response = self.r.table(self.table_name).filter({'id': self.insert_data['id']}).replace(
            self.insert_data
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_replace_durability_soft(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'The Great {name}'.format(name=self.insert_data['name'])})
        response = self.r.table(self.table_name).get(self.insert_data['id']).replace(
            self.insert_data,
            durability='soft'
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_replace_durability_hard(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'The Great {name}'.format(name=self.insert_data['name'])})
        response = self.r.table(self.table_name).get(self.insert_data['id']).replace(
            self.insert_data,
            durability='hard'
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_replace_return_changes(self):
        replace_data = deepcopy(self.insert_data)
        replace_data.update({
            'name': 'The Great {name}'.format(name=self.insert_data['name'])
        })

        self.expected_replace_response['changes'] = [{
            'old_val': self.insert_data,
            'new_val': replace_data
        }]

        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).replace(
            replace_data,
            return_changes=True
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_replace_non_atomic(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        self.insert_data.update({'name': 'The Great {name}'.format(name=self.insert_data['name'])})
        response = self.r.table(self.table_name).get(self.insert_data['id']).replace(
            self.insert_data,
            non_atomic=True
        ).run(self.conn)

        assert response == self.expected_replace_response

    def test_delete_on_table(self):
        self.expected_delete_response['deleted'] = 2
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)
        self.r.table(self.table_name).insert({
            'id': 2,
            'name': 'Iron Man',
            'real_name': 'Anthony Edward Stark',
            'universe': 'Earth-616'
        }).run(self.conn)

        response = self.r.table(self.table_name).delete().run(self.conn)

        assert response == self.expected_delete_response

    def test_delete_by_id(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).delete().run(self.conn)

        assert response == self.expected_delete_response

    def test_delete_durability_soft(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).delete(durability='soft').run(self.conn)

        assert response == self.expected_delete_response

    def test_delete_durability_hard(self):
        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).delete(durability='hard').run(self.conn)

        assert response == self.expected_delete_response

    def test_delete_return_changes(self):
        self.expected_delete_response['changes'] = [{
            'old_val': self.insert_data,
            'new_val': None
        }]

        self.r.table(self.table_name).insert(self.insert_data).run(self.conn)

        response = self.r.table(self.table_name).get(self.insert_data['id']).delete(return_changes=True).run(self.conn)

        assert response == self.expected_delete_response
