package  com.rs.twittertrendsui.vo;

import java.io.Serializable;

public class HashTrend implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String hashTag;
	private Long count;
	
	public String getHashTag() {
		return hashTag;
	}
	public void setHashTag(String hashTag) {
		this.hashTag = hashTag;
	}
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
}
