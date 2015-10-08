package  com.rs.twittertrendsui.rest;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.rs.twittertrendsui.dao.HashTagTrendsHBaseDao;
import com.rs.twittertrendsui.vo.HashTrend;

@Component
@Path("/hashtagtrends")
public class HashTagController {
	
	@Autowired
	HashTagTrendsHBaseDao trendsDao; 
	
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public List<HashTrend> getHashTrends() throws IOException {
		return trendsDao.getHashTrends(1000);
	}
}
