# chainladder-backend

A GraphQL wrapper around `chainladder-python` to act as backend for a larger application.

### Example
**Code in python**
```python
import chainladder as cl
clrd = cl.load_dataset('clrd')
clrd = clrd.loc[clrd['LOB']=='wkcomp', ['CumPaidLoss', 'EarnedPremNet']][clrd.origin>='1995']
clrd = clrd.dropna()
clrd.shape
```

**equivalent code in GraphQL**
```json
mutation {
  loadDataset(name: "clrd"){
    triangle{
      shape
    }
  }
  loc(
    name: "clrd", assignTo: "clrd"
    whereIndex:{key: "LOB", operator: EQ, value:"wkcomp"},
    columns:["CumPaidLoss", "EarnedPremNet"],
    whereOrigin: {operator: GE, value: "1995"}) {
    	triangle {
    		shape
  		}
  }
	dropNa(name: "clrd", assignTo: "clrd"){
    triangle{
      shape
    }
  }
}
```
### Running the Server
```bash
/> python api.py
```
Then navigate to http://localhost:5000/graphql to see the explore the API.
