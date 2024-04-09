import java

from Method m
where m.fromSource() 
    and m.getDeclaringType().hasName("{{ class_name }}")
select m.getQualifiedName(), m.getStringSignature()