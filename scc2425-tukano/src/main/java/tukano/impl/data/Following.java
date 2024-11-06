package tukano.impl.data;

import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Following{
	@Id
	String id;

	String follower;
	String followee;

	Following() {}

	public Following(String follower, String followee) {
		super();
		this.id = "following+" + hashCode() + UUID.randomUUID();

		this.follower = follower;
		this.followee = followee;
	}

	public String getid() {return id;}

	public String getfollower() {
		return follower;
	}

	public void setfollower(String follower) {
		this.follower = follower;
	}

	public String getfollowee() {
		return followee;
	}

	public void setfollowee(String followee) {
		this.followee = followee;
	}

	@Override
	public int hashCode() {
		return Objects.hash(followee, follower);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Following other = (Following) obj;
		return Objects.equals(followee, other.followee) && Objects.equals(follower, other.follower);
	}

	@Override
	public String toString() {
		return "Following [follower=" + follower + ", followee=" + followee + "]";
	}
	
	
}