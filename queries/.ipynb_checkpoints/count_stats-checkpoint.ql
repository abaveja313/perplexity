import java

predicate hasKnownPackage(Callable c) {
  exists(string n |
    n = c.getDeclaringType().getPackage().getName() and
    (
      // Java Builtin
      n.matches("java.lang%") or
      n.matches("java.util%") or
      n.matches("java.io%") or
      n.matches("java.net%") or
      n.matches("java.math%") or
      n.matches("java.time%") or
      // JSON Libraries
      n.matches("org.json%") or
      n.matches("com.google.code.json") or
      // Logging Libraries
      n.matches("org.apache.logging%") or
      n.matches("org.slf4j") or
      // Common Libraries
      n.matches("com.fasterxml.jackson") or
      n.matches("gnu.trove") or
      n.matches("org.apache.commons%") or
      n.matches("com.google.guava%") or
      n.matches("org.joda.time%") or
      n.matches("org.eclipse%")
    )
  )
}

predicate contains(Method target, MethodCall call){
    call.getCaller() = target or call.getCaller().getEnclosingCallable() = target
}

int getClassOtherInvocations(Method targetMethod) {
  result = count(
      MethodCall mCall |
      contains(targetMethod, mCall) and
      mCall.getCallee().getDeclaringType() = targetMethod.getDeclaringType() | mCall)    
}

int getNonClassUnknownInvocations(Method targetMethod){
  result = count(
      MethodCall mCall |
      contains(targetMethod, mCall) and
      mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
      not hasKnownPackage(mCall.getCallee())
  )
}

int getNonClassKnownInvocations(Method targetMethod){
  result = count(
      MethodCall mCall |
      contains(targetMethod, mCall) and
      mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
      hasKnownPackage(mCall.getCallee())
  )
}

int numSpecialFeatures(Method m) {
  result = count(Expr e |
      e.getEnclosingCallable() = m and
      (
          e instanceof LambdaExpr or
          e instanceof VirtualMethodAccess or
          e instanceof FunctionalExpr or
          e instanceof SwitchExpr or
          e instanceof StringTemplateExpr or
          e instanceof MemberRefExpr or
          e instanceof PropertyRefExpr or
          e instanceof UnsafeCoerceExpr or
          e instanceof RecordPatternExpr or
          e instanceof CharacterLiteral or
          e instanceof PreIncExpr or
          e instanceof PostIncExpr
      )
  )
}

from Method m
where
  m.fromSource() and
  m.getDeclaringType().hasName("{{ class_name }}") and m.hasName("{{ method_name }}")
select count(FieldAccess f | f.getEnclosingCallable() = m and f.getField().fromSource() | f) as field_accesses, // field accesses
  count(FieldWrite f | f.getEnclosingCallable() = m and f.getField().fromSource()) as field_writes,
  getClassOtherInvocations(m) as same_class_other_invoc,
  getNonClassKnownInvocations(m) as diff_class_known_invoc,
  getNonClassUnknownInvocations(m) as diff_class_unknown_invoc,
  count(ConditionalStmt cst | cst.getEnclosingCallable() = m | cst) as branch_count,
  numSpecialFeatures(m) as special_count,
  m.getMetrics().getHalsteadLength() as halstead_length,
  m.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
  m.getMetrics().getEfferentCoupling() as efferent_coupling,
  m.getMetrics().getAfferentCoupling() as afferent_coupling,
  m.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index,
  m.getStringSignature() as gsig