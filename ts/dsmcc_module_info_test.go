package ts

import "testing"

func TestDSMCCModuleInfoMetadata(t *testing.T) {
	info := DSMCCModuleInfo{Info: []byte{
		DSMCCModuleDescriptorType, 8, 't', 'e', 'x', 't', '/', 'b', 'm', 'l',
		DSMCCModuleDescriptorName, 9, 'i', 'n', 'd', 'e', 'x', '.', 'b', 'm', 'l',
		DSMCCModuleDescriptorEstimatedDownloadTime, 4, 0, 0, 0, 3,
		DSMCCModuleDescriptorCachingPriority, 1, 80,
		DSMCCModuleDescriptorCompressionType, 5, 1, 0, 0, 4, 0,
	}}
	metadata, ok := info.Metadata()
	if !ok {
		t.Fatal("Metadata rejected valid descriptors")
	}
	if metadata.Type != "text/bml" || metadata.Name != "index.bml" {
		t.Fatalf("metadata type/name = %q, %q", metadata.Type, metadata.Name)
	}
	if metadata.EstimatedDownloadSeconds == nil || *metadata.EstimatedDownloadSeconds != 3 {
		t.Fatalf("estimated download = %v", metadata.EstimatedDownloadSeconds)
	}
	if metadata.CachingPriority == nil || *metadata.CachingPriority != 80 {
		t.Fatalf("caching priority = %v", metadata.CachingPriority)
	}
	if metadata.CompressionType == nil || *metadata.CompressionType != 1 || metadata.OriginalSize == nil || *metadata.OriginalSize != 1024 {
		t.Fatalf("compression = %v, size = %v", metadata.CompressionType, metadata.OriginalSize)
	}
}

func TestDSMCCModuleInfoMetadataRejectsTruncatedDescriptor(t *testing.T) {
	if _, ok := (DSMCCModuleInfo{Info: []byte{DSMCCModuleDescriptorName, 4, 'x'}}).Metadata(); ok {
		t.Fatal("Metadata accepted truncated descriptor")
	}
}
