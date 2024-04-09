import java

predicate isCommon(Callable c) {
  exists(Package p |
    p = c.getDeclaringType().getPackage() and
    (
      p.getName().matches("java.lang%") or
      p.getName().matches("java.util%") or
      p.getName().matches("java.io%") or
      p.getName().matches("java.net%") or
      p.getName().matches("java.math%") or
      p.getName().matches("java.time%") or
      p.getName().matches("org.json%") or
      p.getName().matches("org.apache.commons%")
    )
  )
}

boolean hasUnsupportedCalls(Method m){
    if exists(Call c | c.getCaller() = m and
    not c.getCallee().fromSource() and
    not isCommon(c.getCallee()))
    then result = true
    else result = false
}

from Method m
where m.fromSource() 
    and m.paramsString().matches("{{ params }}")
    and m.hasName("{{ method_name }}")
    and m.getDeclaringType().hasName("{{ class_name }}")
select hasUnsupportedCalls(m),
  count(Call c |
    c.getCaller() = m and
    not c.getCallee().fromSource() and
    isCommon(c.getCallee())
  )