//
// Copyright (C) 2004-2006, Maciej Sobczak
//
// Permission to copy, use, modify, sell and distribute this software
// is granted provided this copyright notice appears in all copies.
// This software is provided "as is" without express or implied
// warranty, and with no claim as to its suitability for any purpose.
//

#include "cpptcl.h"
#include <map>
#include <sstream>
#include <iterator>


using namespace Tcl;
using namespace Tcl::details;
using namespace std;
using namespace boost;


result::result(Tcl_Interp *interp) : interp_(interp) {}

result::operator bool() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     int val, cc;
     cc = Tcl_GetBooleanFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return static_cast<bool>(val);
}

result::operator double() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     double val;
     int cc = Tcl_GetDoubleFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return val;
}

result::operator int() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     int val, cc;
     cc = Tcl_GetIntFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return val;
}

result::operator long() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     long val;
     int cc;
     cc = Tcl_GetLongFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return val;
}

result::operator unsigned int() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     int val;
     int cc;
     cc = Tcl_GetIntFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return (unsigned int)val;
}

result::operator ODate() const
{
	
     Tcl_Obj *obj_ = Tcl_GetObjResult(interp_);
     object o(obj_);
     interpreter interp(interp_,false);
     return o.get<ODate>(interp);
     /*o.set_interp(interp_);
     if(o.length(interp_)!=7) throw tcl_error(interp_);
     ODate od;
     for(int i=0;i<7;i++) {
     	object obj(o.at(interp_,i));
      int val = obj.get<int>(interp_);
		  od.date[i]=val;
		 }
     return od;*/
}

result::operator Tcl_WideInt() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     
     Tcl_WideInt val;
     int cc;
     cc = Tcl_GetWideIntFromObj(interp_, obj, &val);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
     
     return val;
}

result::operator string() const
{
     Tcl_Obj *obj = Tcl_GetObjResult(interp_);
     return Tcl_GetString(obj);
}

result::operator object() const
{
     return object(Tcl_GetObjResult(interp_));
}


void details::set_result(Tcl_Interp *interp, bool b)
{
     Tcl_SetObjResult(interp, Tcl_NewBooleanObj(b));
}

void details::set_result(Tcl_Interp *interp, int i)
{
     Tcl_SetObjResult(interp, Tcl_NewIntObj(i));
}

void details::set_result(Tcl_Interp *interp, unsigned int i)
{
     Tcl_SetObjResult(interp, Tcl_NewIntObj((int)i));
}

void details::set_result(Tcl_Interp *interp, long long i)
{
     Tcl_SetObjResult(interp, Tcl_NewWideIntObj(i));
}

void details::set_result(Tcl_Interp *interp, long i)
{
     Tcl_SetObjResult(interp, Tcl_NewLongObj(i));
}

void details::set_result(Tcl_Interp *interp, double d)
{
     Tcl_SetObjResult(interp, Tcl_NewDoubleObj(d));
}

void details::set_result(Tcl_Interp *interp, string const &s)
{
     Tcl_SetObjResult(interp,
          Tcl_NewStringObj(s.data(), static_cast<int>(s.size())));
}

void details::set_result(Tcl_Interp *interp, const char *s)
{
     Tcl_SetObjResult(interp,
          Tcl_NewStringObj(s, static_cast<int>(strlen(s))));
}

void details::set_result(Tcl_Interp *interp, void *p)
{
     ostringstream ss;
     ss << 'p' << p;
     string s(ss.str());

     Tcl_SetObjResult(interp,
          Tcl_NewStringObj(s.data(), static_cast<int>(s.size())));
}

void details::set_result(Tcl_Interp *interp, object const &o)
{
     Tcl_SetObjResult(interp, o.get_object());
}

void details::set_result(Tcl_Interp *interp_, ODate const &o)
{
     object dt(o);
     /*dt.set_interp(interp_);
     interpreter interp(interp_,false);
     for(int i=0;i<7;i++) {
     	dt.append(interp, object((int)o.date[i]));
		}
		*/
    Tcl_SetObjResult(interp_, dt.get_object());
}

void details::check_params_no(int objc, int required)
{
     if (objc < required)
     {
          throw tcl_error("Too few arguments.");
     }
}

object details::get_var_params(Tcl_Interp *interp,
     int objc, Tcl_Obj * CONST objv[],
     int from, policies const &pol)
{
     object o;
     if (pol.variadic_)
     {
          check_params_no(objc, from);
          o.assign(objv + from, objv + objc);
     }
     else
     {
          check_params_no(objc, from + 1);
          o.assign(objv[from]);
     }

     o.set_interp(interp);

     return o;
}


namespace // anonymous
{

// map of polymorphic callbacks
typedef map<string, shared_ptr<callback_base> > callback_interp_map;
typedef map<Tcl_Interp *, callback_interp_map> callback_map;

callback_map callbacks;
callback_map constructors;

// map of call policies
typedef map<string, policies> policies_interp_map;
typedef map<Tcl_Interp *, policies_interp_map> policies_map;

policies_map call_policies;

// map of object handlers
typedef map<string, shared_ptr<class_handler_base> > class_interp_map;
typedef map<Tcl_Interp *, class_interp_map> class_handlers_map;

class_handlers_map class_handlers;


// helper for finding call policies - returns true when found
bool find_policies(Tcl_Interp *interp, string const &cmdName,
     policies_interp_map::iterator &piti)
{
     policies_map::iterator pit = call_policies.find(interp);
     if (pit == call_policies.end())
     {
          return false;
     }
     
     piti = pit->second.find(cmdName);
     return piti != pit->second.end();
}

extern "C"
int object_handler(ClientData cd, Tcl_Interp *interp,
     int objc, Tcl_Obj * CONST objv[]);

// helper function for post-processing call policies
// for both free functions (isMethod == false)
// and class methods (isMethod == true)
void post_process_policies(Tcl_Interp *interp, policies &pol,
     Tcl_Obj * CONST objv[], bool isMethod)
{
     // check if it is a factory
     if (!pol.factory_.empty())
     {
          class_handlers_map::iterator it = class_handlers.find(interp);
          if (it == class_handlers.end())
          {
               throw tcl_error(
                    "Factory was registered for unknown class.");
          }

          class_interp_map::iterator oit = it->second.find(pol.factory_);
          if (oit == it->second.end())
          {
               throw tcl_error(
                    "Factory was registered for unknown class.");
          }

          class_handler_base *chb = oit->second.get();

          // register a new command for the object returned
          // by this factory function
          // if everything went OK, the result is the address of the
          // new object in the 'pXXX' form
          // - the new command will be created with this name

          Tcl_CreateObjCommand(interp,
               Tcl_GetString(Tcl_GetObjResult(interp)),
               object_handler, static_cast<ClientData>(chb), 0);
     }

     // process all declared sinks
     // - unregister all object commands that envelopes the pointers
     for (vector<int>::iterator s = pol.sinks_.begin();
          s != pol.sinks_.end(); ++s)
     {
          if (isMethod == false)
          {
               // example: if there is a declared sink at parameter 3,
               // and the Tcl command was:
               // % fun par1 par2 PAR3 par4
               // then the index 3 correctly points into the objv array

               int index = *s;
               Tcl_DeleteCommand(interp, Tcl_GetString(objv[index]));
          }
          else
          {
               // example: if there is a declared sink at parameter 3,
               // and the Tcl command was:
               // % $p method par1 par2 PAR3 par4
               // then the index 3 needs to be incremented
               // in order correctly point into the 4th index of objv array

               int index = *s + 1;
               Tcl_DeleteCommand(interp, Tcl_GetString(objv[index]));
          }
     }
}

// actual functions handling various callbacks

// generic callback handler
extern "C"
int callback_handler(ClientData, Tcl_Interp *interp,
     int objc, Tcl_Obj * CONST objv[])
{
     callback_map::iterator it = callbacks.find(interp);
     if (it == callbacks.end())
     {
          Tcl_SetResult(interp,
               "Trying to invoke non-existent callback (wrong interpreter?)",
               TCL_STATIC);
          return TCL_ERROR;
     }
     
     string cmdName(Tcl_GetString(objv[0]));
     callback_interp_map::iterator iti = it->second.find(cmdName);
     if (iti == it->second.end())
     {
          Tcl_SetResult(interp,
               "Trying to invoke non-existent callback (wrong cmd name?)",
               TCL_STATIC);
          return TCL_ERROR;
     }
     
     policies_map::iterator pit = call_policies.find(interp);
     if (pit == call_policies.end())
     {
          Tcl_SetResult(interp,
               "Trying to invoke callback with no known policies",
               TCL_STATIC);
          return TCL_ERROR;
     }
     
     policies_interp_map::iterator piti;
     if (find_policies(interp, cmdName, piti) == false)
     {
          Tcl_SetResult(interp,
               "Trying to invoke callback with no known policies",
               TCL_STATIC);
          return TCL_ERROR;
     }

     policies &pol = piti->second;
     
     try
     {
          iti->second->invoke(interp, objc, objv, pol);

          post_process_policies(interp, pol, objv, false);
     }
     catch (std::exception const &e)
     {
          Tcl_SetResult(interp, const_cast<char*>(e.what()), TCL_VOLATILE);
          return TCL_ERROR;
     }
     catch (...)
     {
          Tcl_SetResult(interp, "Unknown error.", TCL_STATIC);
          return TCL_ERROR;
     }
     
     return TCL_OK;
}

// generic "object" command handler
extern "C"
int object_handler(ClientData cd, Tcl_Interp *interp,
     int objc, Tcl_Obj * CONST objv[])
{
     // here, client data points to the singleton object
     // which is responsible for managing commands for
     // objects of a given type

     class_handler_base *chb = reinterpret_cast<class_handler_base*>(cd);

     // the command name has the form 'pXXX' where XXX is the address
     // of the "this" object

     string const str(Tcl_GetString(objv[0]));
     istringstream ss(str);
     char dummy;
     void *p;
     ss >> dummy >> p;

     try
     {
          string methodName(Tcl_GetString(objv[1]));
          policies &pol = chb->get_policies(methodName);

          chb->invoke(p, interp, objc, objv, pol);

          post_process_policies(interp, pol, objv, true);
     }
     catch (std::exception const &e)
     {
          Tcl_SetResult(interp, const_cast<char*>(e.what()), TCL_VOLATILE);
          return TCL_ERROR;
     }
     catch (...)
     {
          Tcl_SetResult(interp, "Unknown error.", TCL_STATIC);
          return TCL_ERROR;
     }

     return TCL_OK;
}

// generic "constructor" command
extern "C"
int constructor_handler(ClientData cd, Tcl_Interp *interp,
     int objc, Tcl_Obj * CONST objv[])
{
     // here, client data points to the singleton object
     // which is responsible for managing commands for
     // objects of a given type

     class_handler_base *chb = reinterpret_cast<class_handler_base*>(cd);

     callback_map::iterator it = constructors.find(interp);
     if (it == constructors.end())
     {
          Tcl_SetResult(interp,
               "Trying to invoke non-existent callback (wrong interpreter?)",
               TCL_STATIC);
          return TCL_ERROR;
     }
     
     string className(Tcl_GetString(objv[0]));
     callback_interp_map::iterator iti = it->second.find(className);
     if (iti == it->second.end())
     {
          Tcl_SetResult(interp,
               "Trying to invoke non-existent callback (wrong class name?)",
               TCL_STATIC);
          return TCL_ERROR;
     }
     
     policies_interp_map::iterator piti;
     if (find_policies(interp, className, piti) == false)
     {
          Tcl_SetResult(interp,
               "Trying to invoke callback with no known policies",
               TCL_STATIC);
          return TCL_ERROR;
     }

     policies &pol = piti->second;

     try
     {
          iti->second->invoke(interp, objc, objv, pol);

          // if everything went OK, the result is the address of the
          // new object in the 'pXXX' form
          // - we can create a new command with this name

          Tcl_CreateObjCommand(interp,
               Tcl_GetString(Tcl_GetObjResult(interp)),
               object_handler, static_cast<ClientData>(chb), 0);
     }
     catch (std::exception const &e)
     {
          Tcl_SetResult(interp, const_cast<char*>(e.what()), TCL_VOLATILE);
          return TCL_ERROR;
     }
     catch (...)
     {
          Tcl_SetResult(interp, "Unknown error.", TCL_STATIC);
          return TCL_ERROR;
     }

     return TCL_OK;
}

} // namespace anonymous

Tcl::details::no_init_type Tcl::no_init;


policies & policies::factory(string const &name)
{
     factory_ = name;
     return *this;
}

policies & policies::sink(int index)
{
     sinks_.push_back(index);
     return *this;
}

policies & policies::variadic()
{
     variadic_ = true;
     return *this;
}

policies Tcl::factory(string const &name)
{
     return policies().factory(name);
}

policies Tcl::sink(int index)
{
     return policies().sink(index);
}

policies Tcl::variadic()
{
     return policies().variadic();
}


class_handler_base::class_handler_base()
{
     // default policies for the -delete command
     policies_["-delete"] = policies();
}

void class_handler_base::register_method(string const &name,
     shared_ptr<object_cmd_base> ocb, policies const &p)
{
     methods_[name] = ocb;
     policies_[name] = p;
}

policies & class_handler_base::get_policies(string const &name)
{
     policies_map_type::iterator it = policies_.find(name);
     if (it == policies_.end())
     {
          throw tcl_error("Trying to use non-existent policy: " + name);
     }

     return it->second;
}


object::object()
     : interp_(0)
{
     obj_ = Tcl_NewObj();
     Tcl_IncrRefCount(obj_);
}

object::object(bool b)
     : interp_(0)
{
     obj_ = Tcl_NewBooleanObj(b);
     Tcl_IncrRefCount(obj_);
}

object::object(char const *buf, size_t size)
     : interp_(0)
{
     obj_ = Tcl_NewByteArrayObj(
          reinterpret_cast<unsigned char const *>(buf),
          static_cast<int>(size));
     Tcl_IncrRefCount(obj_);
}

object::object(double d)
     : interp_(0)
{
     obj_ = Tcl_NewDoubleObj(d);
     Tcl_IncrRefCount(obj_);
}

object::object(int i)
     : interp_(0)
{
     obj_ = Tcl_NewIntObj(i);
     Tcl_IncrRefCount(obj_);
}

object::object(long l)
     : interp_(0)
{
     obj_ = Tcl_NewLongObj(l);
     Tcl_IncrRefCount(obj_);
}

object::object(unsigned int i)
     : interp_(0)
{
     obj_ = Tcl_NewIntObj((int)i);
     Tcl_IncrRefCount(obj_);
}

object::object(long long l)
     : interp_(0)
{
     obj_ = Tcl_NewWideIntObj(l);
     Tcl_IncrRefCount(obj_);
}

object::object(char const *s)
     : interp_(0)
{
     obj_ = Tcl_NewStringObj(s, -1);
     Tcl_IncrRefCount(obj_);
}

object::object(string const &s)
     : interp_(0)
{
     obj_ = Tcl_NewStringObj(s.data(), static_cast<int>(s.size()));
     Tcl_IncrRefCount(obj_);
}

object::object(Tcl_Obj *o, bool shared)
     : interp_(0)
{
     init(o, shared);
}

object::object(ODate o)
     : interp_(0)
{
		 obj_ = Tcl_NewObj();
     Tcl_IncrRefCount(obj_);
    //object od;
    //od.set_interp(interp_);
    interpreter interp(interp_,false);
    for(int i=0;i<7;i++) {
     	append(interp, object((int)o.date[i]));
		}
}

object::object(object const &other, bool shared)
     : interp_(other.get_interp())
{
     init(other.obj_, shared);
}

void object::init(Tcl_Obj *o, bool shared)
{
     if (shared)
     {
          obj_ = o;
     }
     else
     {
          obj_ = Tcl_DuplicateObj(o);
     }
     Tcl_IncrRefCount(obj_);
}

object::~object()
{
     Tcl_DecrRefCount(obj_);
}

object & object::assign(bool b)
{
     Tcl_SetBooleanObj(obj_, b);
     return *this;
}

object & object::resize(size_t size)
{
     Tcl_SetByteArrayLength(obj_, static_cast<int>(size));
     return *this;
}

object & object::assign(char const *buf, size_t size)
{
     Tcl_SetByteArrayObj(obj_,
          reinterpret_cast<unsigned char const *>(buf),
          static_cast<int>(size));
     return *this;
}

object & object::assign(double d)
{
     Tcl_SetDoubleObj(obj_, d);
     return *this;
}

object & object::assign(int i)
{
     Tcl_SetIntObj(obj_, i);
     return *this;
}

object & object::assign(long l)
{
     Tcl_SetLongObj(obj_, l);
     return *this;
}

object & object::assign(unsigned int i)
{
     Tcl_SetIntObj(obj_, (int)i);
     return *this;
}

object & object::assign(long long l)
{
     Tcl_SetWideIntObj(obj_, l);
     return *this;
}

object & object::assign(char const *s)
{
     Tcl_SetStringObj(obj_, s, -1);
     return *this;
}

object & object::assign(string const &s)
{
     Tcl_SetStringObj(obj_, s.data(), static_cast<int>(s.size()));
     return *this;
}

object & object::assign(object const &other)
{
     object(other).swap(*this);
     return *this;
}

object & object::assign(ODate const &o)
{
    interpreter interp(interp_,false);
    for(int i=0;i<7;i++) {
     	append(interp, object((int)o.date[i]));
		}
		return *this;
}

object & object::assign(Tcl_Obj *o)
{
     object(o).swap(*this);
     return *this;
}

object & object::swap(object &other)
{
     std::swap(obj_, other.obj_);
     std::swap(interp_, other.interp_);
     return *this;
}

template <>
bool object::get<bool>(interpreter &i) const
{
     int retVal;
     int res = Tcl_GetBooleanFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return static_cast<bool>(retVal);
}

template <>
vector<char> object::get<vector<char> >(interpreter &) const
{
     size_t size;
     char const *buf = get(size);
     return vector<char>(buf, buf + size);
}

template <>
double object::get<double>(interpreter &i) const
{
     double retVal;
     int res = Tcl_GetDoubleFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return retVal;
}

template <>
int object::get<int>(interpreter &i) const
{
     int retVal;

     int res = Tcl_GetIntFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return retVal;
}

template <>
long object::get<long>(interpreter &i) const
{
     long retVal;
     int res = Tcl_GetLongFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return retVal;
}

template <>
unsigned int object::get<unsigned int>(interpreter &i) const
{
     int retVal;
     int res = Tcl_GetIntFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return (unsigned int)retVal;
}

template <>
long long object::get<long long>(interpreter &i) const
{
     Tcl_WideInt retVal;
     int res = Tcl_GetWideIntFromObj(i.get(), obj_, &retVal);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return (long long)retVal;
}


template <>
char const * object::get<char const *>(interpreter &) const
{
     return get();
}

template <>
string object::get<string>(interpreter &) const
{
     int len;
     char const *buf = Tcl_GetStringFromObj(obj_, &len);
     return string(buf, buf + len);
}

template <>
ODate object::get<ODate>(interpreter &interp) const
{
     if(length(interp)!=7) throw tcl_error(interp.get());
     ODate od;
     for(int i=0;i<7;i++) {
     	object obj(at(interp,i));
      int val = obj.get<int>(interp);
		  od.date[i]=val;
		 }
     return od;
}

char const * object::get() const
{
     return Tcl_GetString(obj_);
}

char const * object::get(size_t &size) const
{
     int len;
     unsigned char *buf = Tcl_GetByteArrayFromObj(obj_, &len);
     size = len;
     return const_cast<char const *>(reinterpret_cast<char *>(buf));
}

size_t object::length(interpreter &i) const
{
     int len;
     int res = Tcl_ListObjLength(i.get(), obj_, &len);

     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return static_cast<size_t>(len);
}

object object::at(interpreter &i, size_t index) const
{
     Tcl_Obj *o;
     int res = Tcl_ListObjIndex(i.get(), obj_, static_cast<int>(index), &o);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }
     if (o == NULL)
     {
          throw tcl_error("Index out of range.");
     }

     return object(o);
}

object & object::append(interpreter &i, object const &o)
{
     int res = Tcl_ListObjAppendElement(i.get(), obj_, o.obj_);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return *this;
}

object & object::append_list(interpreter &i, object const &o)
{
     int res = Tcl_ListObjAppendList(i.get(), obj_, o.obj_);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return *this;
}

object & object::replace(interpreter &i, size_t index, size_t count,
     object const &o)
{
     int res = Tcl_ListObjReplace(i.get(), obj_,
          static_cast<int>(index), static_cast<int>(count),
          1, &(o.obj_));
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return *this;
}

object & object::replace_list(interpreter &i, size_t index, size_t count,
     object const &o)
{
     int objc;
     Tcl_Obj **objv;

     int res = Tcl_ListObjGetElements(i.get(), o.obj_, &objc, &objv);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     res = Tcl_ListObjReplace(i.get(), obj_,
          static_cast<int>(index), static_cast<int>(count),
          objc, objv);
     if (res != TCL_OK)
     {
          throw tcl_error(i.get());
     }

     return *this;
}

void object::set_interp(Tcl_Interp *interp)
{
     interp_ = interp;
}

Tcl_Interp * object::get_interp() const
{
     return interp_;
}


interpreter::interpreter()
{
     interp_ =  Tcl_CreateInterp();
     owner_ = true;
}

interpreter::interpreter(Tcl_Interp *interp, bool owner)
{
     interp_ =  interp;
     owner_ = owner;
}

interpreter::~interpreter()
{
     if (owner_)
     {
          // clear all callback info belonging to this interpreter
          clear_definitions(interp_);

          Tcl_DeleteInterp(interp_);
     }
}

void interpreter::make_safe()
{
     int cc = Tcl_MakeSafe(interp_);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
}

result interpreter::eval(string const &script)
{
     int cc = Tcl_Eval(interp_, script.c_str());
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
 
     return result(interp_);
}

result interpreter::eval(istream &s)
{
     string str(
          istreambuf_iterator<char>(s.rdbuf()),
          istreambuf_iterator<char>()
     );
     return eval(str);
}

result interpreter::eval(object const &o)
{
     int cc = Tcl_EvalObjEx(interp_, o.get_object(), 0);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
 
     return result(interp_);
}

void interpreter::pkg_provide(string const &name, string const &version)
{
     int cc = Tcl_PkgProvide(interp_, name.c_str(), version.c_str());
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
}

void interpreter::create_alias(string const &cmd,
     interpreter &targetInterp, string const &targetCmd)
{
     int cc = Tcl_CreateAlias(interp_, cmd.c_str(),
          targetInterp.interp_, targetCmd.c_str(), 0, 0);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp_);
     }
}

void interpreter::clear_definitions(Tcl_Interp *interp)
{
     // delete all callbacks that were registered for given interpreter

     {
          callback_map::iterator it = callbacks.find(interp);
          if (it == callbacks.end())
          {
               // no callbacks defined for this interpreter
               return;
          }

          callback_interp_map &imap = it->second;
          for (callback_interp_map::iterator it2 = imap.begin();
               it2 != imap.end(); ++it2)
          {
               Tcl_DeleteCommand(interp, it2->first.c_str());
          }

          callbacks.erase(interp);
     }

     // delete all constructors

     {
          callback_map::iterator it = constructors.find(interp);
          if (it == constructors.end())
          {
               // no callbacks defined for this interpreter
               return;
          }

          callback_interp_map &imap = it->second;
          for (callback_interp_map::iterator it2 = imap.begin();
               it2 != imap.end(); ++it2)
          {
               Tcl_DeleteCommand(interp, it2->first.c_str());
          }

          callbacks.erase(interp);
     }

     // delete all call policies

     call_policies.erase(interp);

     // delete all object handlers
     // (we have to assume that all living objects were destroyed,
     // otherwise Bad Things will happen)

     class_handlers.erase(interp);
}

void interpreter::add_function(string const &name,
     shared_ptr<callback_base> cb, policies const &p)
{
     Tcl_CreateObjCommand(interp_, name.c_str(),
          callback_handler, 0, 0);
     
     callbacks[interp_][name] = cb;
     call_policies[interp_][name] = p;
}

void interpreter::add_class(string const &name,
     shared_ptr<class_handler_base> chb)
{
     class_handlers[interp_][name] = chb;
}

void interpreter::add_constructor(string const &name,
     shared_ptr<class_handler_base> chb, shared_ptr<callback_base> cb,
     policies const &p)
{
     Tcl_CreateObjCommand(interp_, name.c_str(),
          constructor_handler, static_cast<ClientData>(chb.get()), 0);

     constructors[interp_][name] = cb;
     call_policies[interp_][name] = p;
}


int tcl_cast<int>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     int res;
     int cc = Tcl_GetIntFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return res;
}

long tcl_cast<long>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     long res;
     int cc = Tcl_GetLongFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return res;
}

unsigned int tcl_cast<unsigned int>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     int res;
     int cc = Tcl_GetIntFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return (unsigned int)res;
}

long long tcl_cast<long long>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     Tcl_WideInt res;
     int cc = Tcl_GetWideIntFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return (long long)res;
}

bool tcl_cast<bool>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     int res;
     int cc = Tcl_GetBooleanFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return res != 0;
}

double tcl_cast<double>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     double res;
     int cc = Tcl_GetDoubleFromObj(interp, obj, &res);
     if (cc != TCL_OK)
     {
          throw tcl_error(interp);
     }
     
     return res;
}

string tcl_cast<string>::from(Tcl_Interp *, Tcl_Obj *obj)
{
     return Tcl_GetString(obj);
}

char const * tcl_cast<char const *>::from(Tcl_Interp *, Tcl_Obj *obj)
{
     return Tcl_GetString(obj);
}

object tcl_cast<object>::from(Tcl_Interp *interp, Tcl_Obj *obj)
{
     object o(obj);
     o.set_interp(interp);

     return o;
}

ODate tcl_cast<ODate>::from(Tcl_Interp *interp_, Tcl_Obj *obj)
{
     object o(obj);
     interpreter interp(interp_,false);
     return o.get<ODate>(interp);
     /*
     o.set_interp(interp_);
     interpreter interp(interp_,false);
     if(o.length(interp)!=7) throw tcl_error(interp_);
     ODate od;
     for(int i=0;i<7;i++) {
     	object obj_(o.at(interp,i));
      int val = obj_.get<int>(interp);
		  od.date[i]=val;
		}
    return od;
    */
}
