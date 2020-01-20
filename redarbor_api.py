from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, url
from tornado.escape import json_decode
from elasticsearch import Elasticsearch


class BaseHandlerES(RequestHandler):
    host = 'localhost'
    port = 9200
    index_name = None
    es = None

    def _connect_elasticsearch(self):
        es = Elasticsearch(
            [{'host': self.host,
              'port': self.port}]
        )
        return es

    def create_index(self, index_name=None, mappings=None):
        if not index_name:
            index_name = self.index_name

        created = False
        # index settings
        settings = {
            "mappings": mappings if mappings else {}
        }

        try:
            if not self.es.indices.exists(index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                self.es.indices.create(index=index_name, body=settings)
                print(f'Created Index "{index_name}"')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created

    def initialize(self):
        if not self.index_name:
            self.index_name = self.__class__.__name__

        self.es = self._connect_elasticsearch()
        self.create_index()

    def search_vals_fields(self, dict_vals_search, oper='match', index_name=None):
        if not index_name:
            index_name = self.index_name

        a_search = None
        if dict_vals_search:
            a_search = {'query': {oper: dict_vals_search}}

        res = self.es.search(index=index_name, body=a_search)

        return res

    def insert_doc(self, id, doc_json, index_name=None):
        if not index_name:
            index_name = self.index_name

        outcome = None
        try:
            outcome = self.es.index(index=index_name, id=id, body=doc_json)
        except Exception as ex:
            print(f'Error insertando doc para {index_name} con Id={id}')
            print(str(ex))

        return outcome

    def update_doc(self, id, doc_json, index_name=None):
        if not index_name:
            index_name = self.index_name

        outcome = None
        try:
            outcome = self.es.update(index=index_name, id=id, body={'doc': doc_json})
        except Exception as ex:
            print(f'Error actualizando doc para {index_name} con Id={id}')
            print(str(ex))

        return outcome

    def delete_doc(self, id, index_name=None):
        if not index_name:
            index_name = self.index_name

        outcome = None
        try:
            outcome = self.es.delete(index=index_name, id=id)
        except Exception as ex:
            print(f'Error borrando doc para Id={id} en {index_name}')
            print(str(ex))

        return outcome

    def get_doc_from_index(self, id, index_name=None):
        if not index_name:
            index_name = self.index_name

        a_doc = None
        try:
            a_doc = self.es.get_source(index=index_name, id=id)
        except Exception as ex:
            print(f'Error get_source para Id={id} en índice {index_name}')
            print(str(ex))

        return a_doc

    def get_params(self, *args):
        params = [arg.replace('/', '') for arg in args if arg]
        return params


class HandlerCompany(BaseHandlerES):
    index_name = 'company'
    id_employee = 'CompanyId'
    mapping_es = {
                    "properties": {
                        "CompanyId": {
                            "type": "keyword"
                        },
                        "Email": {
                            "type": "keyword"
                        },
                        "Password": {
                            "type": "keyword"
                        },
                        "PortalId": {
                            "type": "keyword"
                        },
                        "RoleId": {
                            "type": "keyword"
                        },
                        "StatusId": {
                            "type": "keyword"
                        },
                        "Username": {
                            "type": "keyword"
                        },
                    }
                }

    def create_index(self, index_name=None, mappings=None):
        super().create_index(self.index_name, self.mapping_es)

    def employee(self, id):
        return self.get_doc_from_index(id)

    def employees(self):
        resp_es = self.es.search(self.index_name)

        l_emps = [d_emp['_source']
                  for d_emp in resp_es.get('hits', {}).get('hits', {})
                  ]

        return l_emps

    def get(self, *args):
        params = self.get_params(*args)

        if params:
            resp = self.employee(params[0])
        else:
            l_emps = self.employees()
            resp = {self.index_name: l_emps}

        if resp:
            self.write(resp)

    def put(self, *args):
        error = None

        params = self.get_params(*args)
        id = None
        if params:
            id = params[0]

        if id:
            if not self.employee(id):
                error = f'NO existe documento para el empleado con {self.id_employee}={id}'
            else:
                d_json = json_decode(self.request.body)
                resp = self.update_doc(id, d_json)
                if not resp or not resp.get("result") == "updated":
                    error = f'No se ha podido actualizar el employee con {self.id_employee}={id}'
        else:
            error = f'Hay que indicar el Id (atributo "{self.id_employee}") del employee a modificar'

        if error:
            self.set_status(400)
            self.write(error)

    def campos_obligatorios(self):
        return self.mapping_es.get('properties', {})

    def valid_emp(self, d_json):
        ok = True
        for n_fld, v_fld in self.campos_obligatorios().items():
            if not d_json.get(n_fld):
                ok = False
                break

        return ok

    def post(self, *args):
        error = None

        d_json = json_decode(self.request.body)
        emp_ok = self.valid_emp(d_json)
        if emp_ok:
            id = d_json.get(self.id_employee)
            if self.employee(id):
                error = f'Ya existe documento para empleado con {self.id_employee}={id}'
            else:
                resp = self.insert_doc(id, d_json)
                if resp and resp.get("result") == "created":
                    self.write(self.employee(id))
                else:
                    error = f'No se ha podido grabar el employee con {self.id_employee}={id}'
        else:
            error = f'El documento NO contiene todos los campos obligatorios informados ' \
                    f'({", ".join(self.campos_obligatorios())})'

        if error:
            self.set_status(400)
            self.write(error)

    def delete(self, *args):
        error = None

        params = self.get_params(*args)
        id = None
        if params:
            id = params[0]

        if id:
            if not self.employee(id):
                error = f'NO existe documento para el empleado con {self.id_employee}={id}'
            else:
                resp = self.delete_doc(id)
                if not resp or not resp.get("result") == "deleted":
                    error = f'No se ha podido borrar el employee con {self.id_employee}={id}'
        else:
            error = f'Hay que indicar el Id (atributo "{self.id_employee}") del empleado a borrar'

        if error:
            self.set_status(400)
            self.write(error)


def make_app():
    return Application([
        url(r'/api/redarbor/', HandlerCompany),
        url(r'/api/redarbor(\/[0-9]+)?', HandlerCompany),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(9999)
    IOLoop.current().start()
