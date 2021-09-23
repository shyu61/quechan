# quechan
## how to use
### publish
- path: /publish
- method: POST
- params: `{ topic: string, message: string }`
```json
// e.g.
{"topic": "sample", "message": '{"name": "yamada ichiro", "age": "20"}'}
```

### subscribe
- path: /subscribe
- method: GET
- params: `{ topic: string }`
```json
// e.g.
{"topic": "sample"}
```
