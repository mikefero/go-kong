package kong

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestConsumerGroupsService(T *testing.T) {
	runWhenEnterprise(T, ">=2.7.0", requiredFeatures{})
	assert := assert.New(T)

	client, err := NewTestClient(nil, nil)
	assert.Nil(err)
	assert.NotNil(client)

	consumerGroup := &ConsumerGroup{
		Name: String("foo"),
	}

	createdConsumerGroup, err := client.ConsumerGroups.Create(defaultCtx, consumerGroup)
	assert.Nil(err)
	assert.NotNil(createdConsumerGroup)

	consumerGroup, err = client.ConsumerGroups.Get(defaultCtx, createdConsumerGroup.ID)
	assert.Nil(err)
	assert.NotNil(consumerGroup)

	consumerGroup.Name = String("bar")
	consumerGroup, err = client.ConsumerGroups.Update(defaultCtx, consumerGroup)
	assert.Nil(err)
	assert.NotNil(consumerGroup)
	assert.Equal("bar", *consumerGroup.Name)

	err = client.ConsumerGroups.Delete(defaultCtx, createdConsumerGroup.ID)
	assert.Nil(err)

	// ID can be specified
	id := uuid.NewString()
	consumerGroup = &ConsumerGroup{
		Name: String("foo"),
		ID:   String(id),
	}

	createdConsumerGroup, err = client.ConsumerGroups.Create(defaultCtx, consumerGroup)
	assert.Nil(err)
	assert.NotNil(createdConsumerGroup)
	assert.Equal(id, *createdConsumerGroup.ID)

	err = client.ConsumerGroups.Delete(defaultCtx, createdConsumerGroup.ID)
	assert.Nil(err)
}

func TestConsumerGroupWithTags(T *testing.T) {
	runWhenEnterprise(T, ">=2.7.0", requiredFeatures{})
	assert := assert.New(T)

	client, err := NewTestClient(nil, nil)
	assert.Nil(err)
	assert.NotNil(client)

	consumerGroup := &ConsumerGroup{
		Name: String("foo"),
		Tags: StringSlice("tag1", "tag2"),
	}

	createdConsumerGroup, err := client.ConsumerGroups.Create(defaultCtx, consumerGroup)
	assert.Nil(err)
	assert.NotNil(createdConsumerGroup)
	assert.Equal(StringSlice("tag1", "tag2"), createdConsumerGroup.Tags)

	err = client.ConsumerGroups.Delete(defaultCtx, createdConsumerGroup.ID)
	assert.Nil(err)
}

func TestConsumerGroupListEndpoint(T *testing.T) {
	runWhenEnterprise(T, ">=2.7.0", requiredFeatures{})
	assert := assert.New(T)

	client, err := NewTestClient(nil, nil)
	assert.Nil(err)
	assert.NotNil(client)

	// fixtures
	consumerGroups := []*ConsumerGroup{
		{
			Name: String("foo1"),
		},
		{
			Name: String("foo2"),
		},
		{
			Name: String("foo3"),
		},
	}

	// create fixturs
	for i := 0; i < len(consumerGroups); i++ {
		consumerGroup, err := client.ConsumerGroups.Create(defaultCtx, consumerGroups[i])
		assert.Nil(err)
		assert.NotNil(consumerGroup)
		consumerGroups[i] = consumerGroup
	}

	consumerGroupsFromKong, next, err := client.ConsumerGroups.List(defaultCtx, nil)
	assert.Nil(err)
	assert.Nil(next)
	assert.NotNil(consumerGroupsFromKong)
	assert.Equal(3, len(consumerGroupsFromKong))

	// check if we see all consumerGroups
	assert.True(compareConsumerGroups(consumerGroups, consumerGroupsFromKong))

	// Test pagination
	consumerGroupsFromKong = []*ConsumerGroup{}

	// first page
	page1, next, err := client.ConsumerGroups.List(defaultCtx, &ListOpt{Size: 1})
	assert.Nil(err)
	assert.NotNil(next)
	assert.NotNil(page1)
	assert.Equal(1, len(page1))
	consumerGroupsFromKong = append(consumerGroupsFromKong, page1...)

	// last page
	next.Size = 2
	page2, next, err := client.ConsumerGroups.List(defaultCtx, next)
	assert.Nil(err)
	assert.Nil(next)
	assert.NotNil(page2)
	assert.Equal(2, len(page2))
	consumerGroupsFromKong = append(consumerGroupsFromKong, page2...)

	assert.True(compareConsumerGroups(consumerGroups, consumerGroupsFromKong))

	consumerGroups, err = client.ConsumerGroups.ListAll(defaultCtx)
	assert.Nil(err)
	assert.NotNil(consumerGroups)
	assert.Equal(3, len(consumerGroups))

	for i := 0; i < len(consumerGroups); i++ {
		assert.Nil(client.ConsumerGroups.Delete(defaultCtx, consumerGroups[i].ID))
	}
}

func TestConsumerGroupListWithTags(T *testing.T) {
	runWhenEnterprise(T, ">=2.7.0", requiredFeatures{})
	assert := assert.New(T)

	client, err := NewTestClient(nil, nil)
	assert.Nil(err)
	assert.NotNil(client)

	// fixtures
	consumerGroups := []*ConsumerGroup{
		{
			Name: String("user1"),
			Tags: StringSlice("tag1", "tag2"),
		},
		{
			Name: String("user2"),
			Tags: StringSlice("tag2", "tag3"),
		},
		{
			Name: String("user3"),
			Tags: StringSlice("tag1", "tag3"),
		},
		{
			Name: String("user4"),
			Tags: StringSlice("tag1", "tag2"),
		},
		{
			Name: String("user5"),
			Tags: StringSlice("tag2", "tag3"),
		},
		{
			Name: String("user6"),
			Tags: StringSlice("tag1", "tag3"),
		},
	}

	// create fixtures
	for i := 0; i < len(consumerGroups); i++ {
		consumerGroup, err := client.ConsumerGroups.Create(defaultCtx, consumerGroups[i])
		assert.Nil(err)
		assert.NotNil(consumerGroup)
		consumerGroups[i] = consumerGroup
	}

	consumerGroupsFromKong, next, err := client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags: StringSlice("tag1"),
	})
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(4, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags: StringSlice("tag2"),
	})
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(4, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags: StringSlice("tag1", "tag2"),
	})
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(6, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags:         StringSlice("tag1", "tag2"),
		MatchAllTags: true,
	})
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(2, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags: StringSlice("tag1", "tag2"),
		Size: 3,
	})
	assert.Nil(err)
	assert.NotNil(next)
	assert.Equal(3, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, next)
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(3, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, &ListOpt{
		Tags:         StringSlice("tag1", "tag2"),
		MatchAllTags: true,
		Size:         1,
	})
	assert.Nil(err)
	assert.NotNil(next)
	assert.Equal(1, len(consumerGroupsFromKong))

	consumerGroupsFromKong, next, err = client.ConsumerGroups.List(defaultCtx, next)
	assert.Nil(err)
	assert.Nil(next)
	assert.Equal(1, len(consumerGroupsFromKong))

	for i := 0; i < len(consumerGroups); i++ {
		assert.Nil(client.ConsumerGroups.Delete(defaultCtx, consumerGroups[i].Name))
	}
}

func compareConsumerGroups(expected, actual []*ConsumerGroup) bool {
	var expectedNames, actualNames []string
	for _, consumerGroup := range expected {
		expectedNames = append(expectedNames, *consumerGroup.Name)
	}

	for _, consumerGroup := range actual {
		actualNames = append(actualNames, *consumerGroup.Name)
	}

	return (compareSlices(expectedNames, actualNames))
}
