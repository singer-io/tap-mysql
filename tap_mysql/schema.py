import attr

STANDARD_KEYS = [
    'sql_datatype',
    'selected',
    'inclusion',
    'description',
    'minimum',
    'maximum',
    'exclusiveMinimum',
    'exclusiveMaximum',
    'multipleOf',
    'maxLength',
    'format'
] 

@attr.s
class Schema(object):

    type = attr.ib(default=None)
    properties = attr.ib(default={})
    sql_datatype = attr.ib(default=None)
    selected = attr.ib(default=None)
    inclusion = attr.ib(default=None)
    description = attr.ib(default=None)
    minimum = attr.ib(default=None)
    maximum = attr.ib(default=None)
    exclusiveMinimum = attr.ib(default=None)
    exclusiveMaximum = attr.ib(default=None)
    multipleOf = attr.ib(default=None)
    maxLength = attr.ib(default=None)
    format = attr.ib(default=None)
    
    def __str__(self):
        return json.dumps(self.to_json())
    
    def to_json(self):
        result = {}
        if self.properties:
            result['properties'] = {
                k: v.to_json() for k, v in self.properties.items()
            }
        if not self.type:
            raise ValueError("Type is required")

        for key in STANDARD_KEYS:
            if self.__dict__[key] is not None:
                result[key] = self.__dict__[key]

        return result

def load_schema(raw):

    kwargs = {}
    if 'properties' in raw:
        kwargs['properties'] = {
            k: load_schema(v) for k, v in raw['properties'].items()
        }
    for key in STANDARD_KEYS:
        if key in raw:
            kwargs[key] = raw[key]
    return Schema(**kwargs)
