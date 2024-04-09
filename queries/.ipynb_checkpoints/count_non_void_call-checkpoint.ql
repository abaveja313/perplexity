import java

from Method m
where m.fromSource() 
    and m.hasName("{{ method_name }}") 
    and m.getDeclaringType().hasName("{{ class_name }}")
select count(MethodCall c | c.getCaller() = m and c.getCallee().fromSource() and c.getCallee().getReturnType().hasName("void") | c)