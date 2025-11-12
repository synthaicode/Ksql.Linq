## options-validation-timeouts

- Purpose: Show how to set validation behavior and schema registration timeouts via `Application.KsqlContextBuilder`.

### Run
```
cd examples/options-validation-timeouts
dotnet run
```

### Highlights
- `ConfigureValidation(autoRegister: false, failOnErrors: false, enablePreWarming: false)`
- `WithTimeouts(schemaRegistrationTimeout: TimeSpan.FromSeconds(60))`

