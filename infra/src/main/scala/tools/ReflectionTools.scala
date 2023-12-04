package tools

object ReflectionTools {

  def callMethod0(obj: Object, method: String): Object = {
    val objClass = obj.getClass
    val objMethod = objClass.getDeclaredMethod(method)
    objMethod.setAccessible(true)
    objMethod.invoke(obj)
  }

  def callMethod1(obj: Object, method: String, arg1cls: Class[_], arg1: Object): Object = {
    val objClass = obj.getClass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1)
  }

  def callMethod2(obj: Object, method: String, arg1cls: Class[_], arg1: Object, arg2cls: Class[_], arg2: Object): Object = {
    val objClass = obj.getClass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls, arg2cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1, arg2)
  }

  def callMethod2(obj: Object, method: String, arg1cls: Class[_], arg1: Object, arg2cls: Class[_]): Object = {
    val objClass = obj.getClass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1)
  }

  def callStatMethod2(objClass: Class[_], method: String, arg1cls: Class[_], arg1: Object, arg2cls: Class[_], arg2: Object): Object = {
    val methods = objClass.getMethods
    for (objMethod <- methods) {
      if (method.equals(objMethod.getName) && objMethod.getParameterCount == 2 && objMethod.getParameterTypes()(0).equals(arg1cls)) {
        println(s"found method:${objMethod.getName} ${objMethod.getParameterCount}")
        objMethod.setAccessible(true)
        try {
          val result = objMethod.invoke(null, arg1, arg2);
          return result;
        } catch {
          case e: Throwable => println(e.getCause)
        }
      }
    }
    throw new UnsupportedOperationException();
  }

  def callParentMethod0(obj: Object, method: String): Object = {
    var objClass = obj.getClass.getSuperclass
    val objMethod = objClass.getDeclaredMethod(method)
    objMethod.setAccessible(true)
    objMethod.invoke(obj)
  }

  def callParentMethod1(obj: Object, method: String, arg1cls: Class[_], arg1: Object): Object = {
    val objClass = obj.getClass.getSuperclass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1)
  }

  def callParentMethod2(obj: Object, method: String, arg1cls: Class[_], arg2cls: Class[_], arg1: Object, arg2: Object): Object = {
    val objClass = obj.getClass.getSuperclass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls, arg2cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1, arg2)
  }

  def callPParentMethod2(obj: Object, method: String, arg1cls: Class[_], arg2cls: Class[_],  arg1: Object, arg2: Object): Object = {
    val objClass = obj.getClass.getSuperclass.getSuperclass
    val objMethod = objClass.getDeclaredMethod(method, arg1cls, arg2cls)
    objMethod.setAccessible(true)
    objMethod.invoke(obj, arg1, arg2)
  }
  
  def getField(obj: Object, fieldName: String): Object = {
    var objClass = obj.getClass
    val field = objClass.getDeclaredField(fieldName) //NoSuchFieldException
    field.setAccessible(true)
    field.get(obj)
  }

  def getParentField(obj: Object, fieldName: String): Object = {
    var objClass = obj.getClass.getSuperclass
    val field = objClass.getDeclaredField(fieldName) //NoSuchFieldException
    field.setAccessible(true)
    field.get(obj)
  }

  def getPParentField(obj: Object, fieldName: String): Object = {
    var objClass = obj.getClass.getSuperclass.getSuperclass
    val field = objClass.getDeclaredField(fieldName) //NoSuchFieldException
    field.setAccessible(true)
    field.get(obj)
  }





}
