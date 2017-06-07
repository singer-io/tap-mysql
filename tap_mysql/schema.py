import attr

@attr.s
class Schema(object):

    type = attr.ib()
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
        if self.type:
            result['type'] = self.type
        else:
            raise ValueError("Type is required")

        keys = ['sql_datatype', 'selected', 'inclusion', 'description',
                'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum',
                'multipleOf', 'maxLength', 'format']
        
        for key in keys:
            if self.__dict__[key] is not None:
                result[key] = self.__dict__[key]

        return result

