import java

from Method m
where m.hasName("{{ method_name }}") 
  and m.getDeclaringType().hasName("{{ class_name }}")
select count(FieldAccess f | f.getEnclosingCallable() = m and f.getField().fromSource() and not f.getField().isFinal() | f)