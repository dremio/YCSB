from faker import Faker
from faker.providers import python, misc, internet


class FakeMan(object):
    @staticmethod
    def get_faker(seed):
        fake = Faker()
        fake.seed_instance(seed)
        fake.add_provider(python)
        fake.add_provider(file)
        fake.add_provider(misc)
        fake.add_provider(internet)
        return fake