/*
Package jh implements a shim layer between functions that return values and errors and the httprouter.Handle function signature.

This package allows us to write http handler functions that return concrete values, for example:


	func (srv *server) handle(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
		v, err := db()
		if err != nil {
			return nil, Wrap(err, "db")
		}

		return map[string]string{"val": v}, nil
	}

The Adapter function takes these return values and appropriately sets headers,
and converts the empty interface into a json repsonse.

There are three reasons this package exists. First of all it allows functions
to divorce themselves from the JSON encoding and header setting. Secondly in
returning errors it reduces cases where the explicit return is forgotten in
cases such as:

	if err != nil {
		http.Error(w, "this is a failure", http.StatusInternalServerError)
	}
	// oops, this shouldn't be reached.

Lastly it allows code called by the http view to set status code where
appropriate. For example consider the following database code:

	func insert(id string, val string) error {
		if find(id){
			return NewError("id already exists", http.StatusConflict)
		}
		// do insert
	}

	func update(id, val string) error {
		if !find(id){
			return NewError("id not found", http.StatusNotFound)
		}
		if err := db.Update(id, val); err != nil {
			return errors.Wrap(err, "db update")
		}
	}

The calling function can have the appropriate headers set by using the Adapter function:

	func (srv *server) update(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
		// parse json update from body

		err := update(val)
		if err != nil {
			return nil, Wrap(err, "update failure")
		}

		// ...
	}

Which would return the appropriate http status code according to the underlying error.
*/
package jh
