mvp-relevant:
- add auth check to methods
- we need to prune backups with a particular algorithm, if the schedule changes
  pruning should continue to run on both old and new schedule, but taking backups 
  only according to new schedule
- test rbac checker
--> more tests on rbac methods (coherency, consistency)
- reconcile with resource types  
- separate monitors into new pod
--------------------------------------------------------------
