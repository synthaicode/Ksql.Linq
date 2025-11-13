using Ksql.Linq.Core.Abstractions;
using System;


namespace Ksql.Linq.Core.Modeling;

public class EntityModelBuilder<T> where T : class
{
    private readonly EntityModel _entityModel;
    internal ModelBuilder Owner { get; }

    internal EntityModelBuilder(EntityModel entityModel, ModelBuilder owner)
    {
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
        Owner = owner;
    }
    public EntityModelBuilder<T> OnError(ErrorAction action)
    {
        _entityModel.ErrorAction = action;
        return this;
    }

    public EntityModel GetModel()
    {
        return _entityModel;
    }

    // Removed HasTopicName: topic name must be provided via [Topic] attribute.

    public override string ToString()
    {
        var entityName = _entityModel.EntityType.Name;
        var topicName = _entityModel.TopicName ?? "undefined";
        var keyCount = _entityModel.KeyProperties.Length;
        var validStatus = _entityModel.IsValid ? "valid" : "invalid";

        return $"Entity: {entityName}, Topic: {topicName}, Keys: {keyCount}, Status: {validStatus}";
    }

}
